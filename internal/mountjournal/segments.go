package mountjournal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	segmentHeaderSize   = int64(32)
	recordHeaderSize    = int64(104)
	segmentVersion      = uint32(1)
	recordVersion       = uint32(1)
	maximumMetadataSize = uint32(64 * 1024)
)

var (
	segmentMagic = [8]byte{'I', 'D', 'O', 'U', 'J', 'N', 'L', '1'}
	recordMagic  = [8]byte{'I', 'D', 'O', 'U', 'J', 'R', 'E', 'C'}
)

type segmentRecordMetadata struct {
	ExtentID      string `json:"extentId"`
	OperationID   string `json:"operationId"`
	LogicalOffset int64  `json:"logicalOffset"`
	CreatedAt     int64  `json:"createdAt"`
}

type decodedRecordHeader struct {
	MetadataLength uint32
	PayloadLength  uint64
	PayloadCRC32   uint32
	MetadataCRC32  uint32
	PayloadSHA256  [32]byte
	MetadataSHA256 [32]byte
}

func (j *Journal) AppendData(ctx context.Context, request AppendDataRequest) (extent Extent, err error) {
	request.ExtentID = strings.TrimSpace(request.ExtentID)
	request.OperationID = strings.TrimSpace(request.OperationID)
	if !validIdentifier(request.ExtentID) || !validIdentifier(request.OperationID) || request.LogicalOffset < 0 || len(request.Payload) == 0 {
		return Extent{}, errors.New("invalid journal data extent")
	}
	if int64(len(request.Payload)) > maximumSegmentMaxBytes || request.LogicalOffset > int64(^uint64(0)>>1)-int64(len(request.Payload)) {
		return Extent{}, errors.New("journal data extent exceeds implementation bounds")
	}
	payloadSHA := sha256.Sum256(request.Payload)
	payloadSHAHex := hex.EncodeToString(payloadSHA[:])
	payloadCRC := crc32.ChecksumIEEE(request.Payload)

	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return Extent{}, errors.New("journal is closed")
	}
	if j.reconcileRequired {
		return Extent{}, ErrRecoveryRequired
	}
	recordWritten := false
	metadataCommitted := false
	defer func() {
		if err != nil && recordWritten && !metadataCommitted {
			j.reconcileRequired = true
		}
	}()
	if existing, err := getExtent(ctx, j.db, request.ExtentID); err == nil {
		if existing.OperationID != request.OperationID || existing.Kind != ExtentData || existing.LogicalOffset != request.LogicalOffset ||
			existing.Length != int64(len(request.Payload)) || existing.SHA256 != payloadSHAHex || existing.CRC32 != payloadCRC {
			return Extent{}, ErrIdempotencyReuse
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Extent{}, err
	}
	if found, err := recoveryExistsForExtent(ctx, j.db, request.ExtentID); err != nil {
		return Extent{}, err
	} else if found {
		return Extent{}, ErrRecoveryRequired
	}
	operation, err := getOperation(ctx, j.db, request.OperationID)
	if err != nil {
		return Extent{}, err
	}
	if operation.State == StateRemoteCommitted || operation.State == StateAbandoned || operation.State == StateConflicted {
		return Extent{}, ErrInvalidTransition
	}
	if request.LogicalOffset+int64(len(request.Payload)) > operation.NewSize {
		return Extent{}, errors.New("journal extent exceeds operation logical size")
	}
	now := j.options.Now().UnixNano()
	metadata := segmentRecordMetadata{
		ExtentID: request.ExtentID, OperationID: request.OperationID,
		LogicalOffset: request.LogicalOffset, CreatedAt: now,
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil || len(metadataJSON) > int(maximumMetadataSize) {
		return Extent{}, errors.New("journal record metadata is invalid")
	}
	metadataSHA := sha256.Sum256(metadataJSON)
	recordLength := recordHeaderSize + int64(len(metadataJSON)) + int64(len(request.Payload))
	createdSegment, err := j.ensureActiveSegmentLocked(ctx, recordLength)
	if err != nil {
		return Extent{}, err
	}
	recordOffset := j.activeSize
	payloadOffset := recordOffset + recordHeaderSize + int64(len(metadataJSON))
	header := encodeRecordHeader(decodedRecordHeader{
		MetadataLength: uint32(len(metadataJSON)), PayloadLength: uint64(len(request.Payload)),
		PayloadCRC32: payloadCRC, MetadataCRC32: crc32.ChecksumIEEE(metadataJSON),
		PayloadSHA256: payloadSHA, MetadataSHA256: metadataSHA,
	})
	if err := writeAllAt(j.active, header, recordOffset); err != nil {
		return Extent{}, err
	}
	if err := writeAllAt(j.active, metadataJSON, recordOffset+recordHeaderSize); err != nil {
		return Extent{}, err
	}
	if err := writeAllAt(j.active, request.Payload, payloadOffset); err != nil {
		return Extent{}, err
	}
	recordWritten = true
	j.activeSize += recordLength
	if err := j.callFault(FaultAfterSegmentWrite); err != nil {
		return Extent{}, err
	}
	if err := j.callFault(FaultBeforeSegmentSync); err != nil {
		return Extent{}, err
	}
	if err := j.active.Sync(); err != nil {
		return Extent{}, fmt.Errorf("sync journal segment: %w", err)
	}
	if err := j.callFault(FaultAfterSegmentSync); err != nil {
		return Extent{}, err
	}
	if createdSegment {
		if err := syncJournalDirectory(j.dir); err != nil {
			return Extent{}, fmt.Errorf("sync new journal segment directory: %w", err)
		}
		if err := j.callFault(FaultAfterDirectorySync); err != nil {
			return Extent{}, err
		}
	}
	if err := j.callFault(FaultBeforeMetadataCommit); err != nil {
		return Extent{}, err
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Extent{}, err
	}
	defer tx.Rollback()
	sequence, err := nextExtentSequence(ctx, tx, request.OperationID)
	if err != nil {
		return Extent{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO extents (
 id, operation_id, sequence, kind, logical_offset, length, segment_id,
 record_offset, record_length, payload_offset, crc32, sha256, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		request.ExtentID, request.OperationID, sequence, ExtentData,
		request.LogicalOffset, len(request.Payload), j.activeID, recordOffset,
		recordLength, payloadOffset, int64(payloadCRC), payloadSHAHex, now)
	if err != nil {
		return Extent{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE segments SET observed_size = ?, updated_at = ? WHERE id = ?;`, j.activeSize, now, j.activeID); err != nil {
		return Extent{}, err
	}
	if err := markOperationLocallyDurable(ctx, tx, request.OperationID, operation.State, now); err != nil {
		return Extent{}, err
	}
	if err := appendEvent(ctx, tx, request.OperationID, "extent_append", StateLocalDurable, request.ExtentID, now); err != nil {
		return Extent{}, err
	}
	if err := tx.Commit(); err != nil {
		return Extent{}, err
	}
	metadataCommitted = true
	extent = Extent{
		ID: request.ExtentID, OperationID: request.OperationID, Sequence: sequence,
		Kind: ExtentData, LogicalOffset: request.LogicalOffset, Length: int64(len(request.Payload)),
		SegmentID: j.activeID, RecordOffset: recordOffset, RecordLength: recordLength,
		PayloadOffset: payloadOffset, CRC32: payloadCRC, SHA256: payloadSHAHex, CreatedAt: now,
	}
	if err := j.callFault(FaultAfterMetadataCommit); err != nil {
		return Extent{}, err
	}
	return extent, nil
}

func (j *Journal) AppendSparse(ctx context.Context, request AppendSparseRequest) (Extent, error) {
	request.ExtentID = strings.TrimSpace(request.ExtentID)
	request.OperationID = strings.TrimSpace(request.OperationID)
	request.Kind = strings.TrimSpace(request.Kind)
	if !validIdentifier(request.ExtentID) || !validIdentifier(request.OperationID) ||
		(request.Kind != ExtentZero && request.Kind != ExtentHole) || request.LogicalOffset < 0 || request.Length <= 0 ||
		request.LogicalOffset > int64(^uint64(0)>>1)-request.Length {
		return Extent{}, errors.New("invalid journal sparse extent")
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Extent{}, err
	}
	defer tx.Rollback()
	if existing, err := getExtent(ctx, tx, request.ExtentID); err == nil {
		if existing.OperationID != request.OperationID || existing.Kind != request.Kind ||
			existing.LogicalOffset != request.LogicalOffset || existing.Length != request.Length {
			return Extent{}, ErrIdempotencyReuse
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Extent{}, err
	}
	operation, err := getOperation(ctx, tx, request.OperationID)
	if err != nil {
		return Extent{}, err
	}
	if request.LogicalOffset+request.Length > operation.NewSize || operation.State == StateRemoteCommitted || operation.State == StateAbandoned {
		return Extent{}, ErrInvalidTransition
	}
	sequence, err := nextExtentSequence(ctx, tx, request.OperationID)
	if err != nil {
		return Extent{}, err
	}
	now := j.options.Now().UnixNano()
	_, err = tx.ExecContext(ctx, `INSERT INTO extents (
 id, operation_id, sequence, kind, logical_offset, length, segment_id,
 record_offset, record_length, payload_offset, crc32, sha256, created_at
) VALUES (?, ?, ?, ?, ?, ?, NULL, 0, 0, 0, 0, '', ?);`,
		request.ExtentID, request.OperationID, sequence, request.Kind,
		request.LogicalOffset, request.Length, now)
	if err != nil {
		return Extent{}, err
	}
	if err := markOperationLocallyDurable(ctx, tx, request.OperationID, operation.State, now); err != nil {
		return Extent{}, err
	}
	if err := appendEvent(ctx, tx, request.OperationID, "extent_append", StateLocalDurable, request.ExtentID, now); err != nil {
		return Extent{}, err
	}
	if err := tx.Commit(); err != nil {
		return Extent{}, err
	}
	return Extent{
		ID: request.ExtentID, OperationID: request.OperationID, Sequence: sequence,
		Kind: request.Kind, LogicalOffset: request.LogicalOffset, Length: request.Length,
		CreatedAt: now,
	}, nil
}

func markOperationLocallyDurable(ctx context.Context, tx *sql.Tx, operationID string, currentState string, now int64) error {
	if currentState != StatePending && currentState != StateRecovery {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE operations SET state = ?, last_error = '', updated_at = ? WHERE id = ? AND state = ?;`, StateLocalDurable, now, operationID, currentState)
	return err
}

func nextExtentSequence(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, operationID string) (int64, error) {
	var sequence int64
	err := query.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM extents WHERE operation_id = ?;`, operationID).Scan(&sequence)
	return sequence, err
}

func (j *Journal) ensureActiveSegmentLocked(ctx context.Context, recordLength int64) (bool, error) {
	if j.active != nil && j.activeSize > segmentHeaderSize && j.activeSize+recordLength > j.options.SegmentMaxBytes {
		now := j.options.Now().UnixNano()
		if _, err := j.db.ExecContext(ctx, `UPDATE segments SET state = 'sealed', observed_size = ?, updated_at = ? WHERE id = ? AND state = 'active';`, j.activeSize, now, j.activeID); err != nil {
			return false, err
		}
		if err := j.active.Close(); err != nil {
			return false, err
		}
		j.active, j.activeID, j.activeSize = nil, "", 0
	}
	if j.active != nil {
		return false, nil
	}
	rows, err := j.db.QueryContext(ctx, `SELECT id, file_name, observed_size FROM segments WHERE state = 'active' ORDER BY created_at DESC, id DESC LIMIT 1;`)
	if err != nil {
		return false, err
	}
	if rows.Next() {
		var id, fileName string
		var observed int64
		if err := rows.Scan(&id, &fileName, &observed); err != nil {
			_ = rows.Close()
			return false, err
		}
		_ = rows.Close()
		file, err := os.OpenFile(filepath.Join(j.dir, fileName), os.O_RDWR, 0o600)
		if err == nil {
			info, statErr := file.Stat()
			if statErr == nil && info.Size() >= segmentHeaderSize && info.Size() == observed {
				j.active, j.activeID, j.activeSize = file, id, info.Size()
				return false, nil
			}
			_ = file.Close()
		}
	} else {
		_ = rows.Close()
	}
	return true, j.createSegmentLocked(ctx)
}

func (j *Journal) createSegmentLocked(ctx context.Context) error {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return err
	}
	id := hex.EncodeToString(random)
	fileName := "segment-" + id + ".dat"
	path := filepath.Join(j.dir, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	header := make([]byte, segmentHeaderSize)
	copy(header[0:8], segmentMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], segmentVersion)
	binary.LittleEndian.PutUint32(header[12:16], uint32(segmentHeaderSize))
	copy(header[16:32], random)
	if err := writeAllAt(file, header, 0); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := syncJournalDirectory(j.dir); err != nil {
		_ = file.Close()
		return err
	}
	now := j.options.Now().UnixNano()
	if _, err := j.db.ExecContext(ctx, `INSERT INTO segments (id, file_name, state, observed_size, created_at, updated_at) VALUES (?, ?, 'active', ?, ?, ?);`, id, fileName, segmentHeaderSize, now, now); err != nil {
		_ = file.Close()
		return err
	}
	j.active, j.activeID, j.activeSize = file, id, segmentHeaderSize
	return nil
}

func encodeRecordHeader(record decodedRecordHeader) []byte {
	header := make([]byte, recordHeaderSize)
	copy(header[0:8], recordMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], recordVersion)
	binary.LittleEndian.PutUint32(header[12:16], uint32(recordHeaderSize))
	binary.LittleEndian.PutUint32(header[16:20], record.MetadataLength)
	binary.LittleEndian.PutUint64(header[24:32], record.PayloadLength)
	binary.LittleEndian.PutUint32(header[32:36], record.PayloadCRC32)
	binary.LittleEndian.PutUint32(header[36:40], record.MetadataCRC32)
	copy(header[40:72], record.PayloadSHA256[:])
	copy(header[72:104], record.MetadataSHA256[:])
	return header
}

func decodeRecordHeader(header []byte) (decodedRecordHeader, error) {
	if int64(len(header)) != recordHeaderSize || string(header[0:8]) != string(recordMagic[:]) ||
		binary.LittleEndian.Uint32(header[8:12]) != recordVersion ||
		binary.LittleEndian.Uint32(header[12:16]) != uint32(recordHeaderSize) {
		return decodedRecordHeader{}, ErrExtentCorrupt
	}
	record := decodedRecordHeader{
		MetadataLength: binary.LittleEndian.Uint32(header[16:20]),
		PayloadLength:  binary.LittleEndian.Uint64(header[24:32]),
		PayloadCRC32:   binary.LittleEndian.Uint32(header[32:36]),
		MetadataCRC32:  binary.LittleEndian.Uint32(header[36:40]),
	}
	copy(record.PayloadSHA256[:], header[40:72])
	copy(record.MetadataSHA256[:], header[72:104])
	if record.MetadataLength == 0 || record.MetadataLength > maximumMetadataSize || record.PayloadLength == 0 || record.PayloadLength > uint64(maximumSegmentMaxBytes) {
		return decodedRecordHeader{}, ErrExtentCorrupt
	}
	return record, nil
}

func writeAllAt(file *os.File, payload []byte, offset int64) error {
	for len(payload) > 0 {
		written, err := file.WriteAt(payload, offset)
		offset += int64(written)
		payload = payload[written:]
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}

func (j *Journal) ListExtents(ctx context.Context, operationID string) ([]Extent, error) {
	return listExtents(ctx, j.db, operationID)
}

func listExtents(ctx context.Context, query interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, operationID string) ([]Extent, error) {
	rows, err := query.QueryContext(ctx, `SELECT id, operation_id, sequence, kind, logical_offset, length,
 segment_id, record_offset, record_length, payload_offset, crc32, sha256, created_at
 FROM extents WHERE operation_id = ? ORDER BY sequence ASC;`, strings.TrimSpace(operationID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	extents := make([]Extent, 0)
	for rows.Next() {
		extent, err := scanExtent(rows)
		if err != nil {
			return nil, err
		}
		extents = append(extents, extent)
	}
	return extents, rows.Err()
}

type rowScanner interface {
	Scan(...any) error
}

func scanExtent(row rowScanner) (Extent, error) {
	var extent Extent
	var segment sql.NullString
	var checksum int64
	err := row.Scan(
		&extent.ID, &extent.OperationID, &extent.Sequence, &extent.Kind,
		&extent.LogicalOffset, &extent.Length, &segment, &extent.RecordOffset,
		&extent.RecordLength, &extent.PayloadOffset, &checksum, &extent.SHA256,
		&extent.CreatedAt,
	)
	extent.SegmentID = segment.String
	extent.CRC32 = uint32(checksum)
	return extent, err
}

func getExtent(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, extentID string) (Extent, error) {
	return scanExtent(query.QueryRowContext(ctx, `SELECT id, operation_id, sequence, kind, logical_offset, length,
 segment_id, record_offset, record_length, payload_offset, crc32, sha256, created_at
 FROM extents WHERE id = ?;`, strings.TrimSpace(extentID)))
}

func (j *Journal) readExtentRange(extent Extent, destination []byte, relativeOffset int64) error {
	if extent.Kind != ExtentData || extent.SegmentID == "" || relativeOffset < 0 || relativeOffset+int64(len(destination)) > extent.Length {
		return ErrExtentCorrupt
	}
	path, err := segmentPathForID(j.dir, j.db, extent.SegmentID)
	if err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return ErrExtentCorrupt
	}
	defer file.Close()
	if err := verifyExtentPayload(file, extent); err != nil {
		return err
	}
	read, err := file.ReadAt(destination, extent.PayloadOffset+relativeOffset)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if read != len(destination) {
		return ErrExtentCorrupt
	}
	return nil
}

func verifyExtentPayload(file *os.File, extent Extent) error {
	hash := sha256.New()
	checksum := crc32.NewIEEE()
	reader := io.NewSectionReader(file, extent.PayloadOffset, extent.Length)
	buffer := make([]byte, 1024*1024)
	written, err := io.CopyBuffer(io.MultiWriter(hash, checksum), reader, buffer)
	if err != nil || written != extent.Length || hex.EncodeToString(hash.Sum(nil)) != extent.SHA256 || checksum.Sum32() != extent.CRC32 {
		return ErrExtentCorrupt
	}
	return nil
}

func segmentPathForID(dir string, db *sql.DB, segmentID string) (string, error) {
	var fileName string
	if err := db.QueryRow(`SELECT file_name FROM segments WHERE id = ?;`, segmentID).Scan(&fileName); err != nil {
		return "", err
	}
	if filepath.Base(fileName) != fileName || !strings.HasPrefix(fileName, "segment-") || !strings.HasSuffix(fileName, ".dat") {
		return "", ErrExtentCorrupt
	}
	return filepath.Join(dir, fileName), nil
}
