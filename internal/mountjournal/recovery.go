package mountjournal

import (
	"context"
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

func (j *Journal) reconcile(ctx context.Context) error {
	referenced, err := loadReferencedExtents(ctx, j.db)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(referenced))
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !validSegmentFileName(entry.Name()) {
			continue
		}
		segmentID := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "segment-"), ".dat")
		path := filepath.Join(j.dir, entry.Name())
		if err := j.reconcileSegment(ctx, path, entry.Name(), segmentID, referenced, seen); err != nil {
			return err
		}
	}
	for key, extent := range referenced {
		if _, ok := seen[key]; ok {
			continue
		}
		recovery := RecoveryRecord{
			ID:       recoveryID(extent.SegmentID, extent.RecordOffset, extent.ID),
			ExtentID: extent.ID, OperationID: extent.OperationID, SegmentID: extent.SegmentID,
			Kind: RecoveryMissing, RecordOffset: extent.RecordOffset,
			PayloadOffset: extent.PayloadOffset, PayloadLength: extent.Length,
			LogicalOffset: extent.LogicalOffset, CRC32: extent.CRC32, SHA256: extent.SHA256,
			Detail: "committed extent references unavailable segment bytes",
		}
		if err := j.recordRecovery(ctx, recovery); err != nil {
			return err
		}
	}
	return nil
}

func loadReferencedExtents(ctx context.Context, db *sql.DB) (map[string]Extent, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, operation_id, sequence, kind, logical_offset, length,
 segment_id, record_offset, record_length, payload_offset, crc32, sha256, created_at
 FROM extents WHERE kind = 'data' ORDER BY segment_id, record_offset;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]Extent)
	for rows.Next() {
		extent, err := scanExtent(rows)
		if err != nil {
			return nil, err
		}
		result[segmentRecordKey(extent.SegmentID, extent.RecordOffset)] = extent
	}
	return result, rows.Err()
}

func (j *Journal) reconcileSegment(ctx context.Context, path string, fileName string, segmentID string, referenced map[string]Extent, seen map[string]struct{}) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	now := j.options.Now().UnixNano()
	if info.Size() < segmentHeaderSize {
		if err := j.upsertSegment(ctx, segmentID, fileName, "corrupt", info.Size(), now); err != nil {
			return err
		}
		return j.recordRecovery(ctx, RecoveryRecord{
			ID: recoveryID(segmentID, 0, "segment"), SegmentID: segmentID,
			Kind: RecoveryIncomplete, AvailableBytes: info.Size(),
			Detail: "segment header is incomplete",
		})
	}
	header := make([]byte, segmentHeaderSize)
	if _, err := file.ReadAt(header, 0); err != nil || !validSegmentHeader(header, segmentID) {
		if err := j.upsertSegment(ctx, segmentID, fileName, "corrupt", info.Size(), now); err != nil {
			return err
		}
		return j.recordRecovery(ctx, RecoveryRecord{
			ID: recoveryID(segmentID, 0, "segment"), SegmentID: segmentID,
			Kind: RecoveryCorrupt, AvailableBytes: info.Size(),
			Detail: "segment header failed integrity validation",
		})
	}
	if err := j.upsertSegment(ctx, segmentID, fileName, "sealed", info.Size(), now); err != nil {
		return err
	}
	for offset := segmentHeaderSize; offset < info.Size(); {
		remaining := info.Size() - offset
		key := segmentRecordKey(segmentID, offset)
		if _, found := referenced[key]; found {
			seen[key] = struct{}{}
		}
		if remaining < recordHeaderSize {
			return j.recordSegmentRecovery(ctx, segmentID, RecoveryRecord{
				ID: recoveryID(segmentID, offset, "trailing"), SegmentID: segmentID,
				Kind: RecoveryIncomplete, RecordOffset: offset, AvailableBytes: remaining,
				Detail: "trailing record header is incomplete",
			})
		}
		recordHeaderBytes := make([]byte, recordHeaderSize)
		if _, err := file.ReadAt(recordHeaderBytes, offset); err != nil {
			return err
		}
		recordHeader, err := decodeRecordHeader(recordHeaderBytes)
		if err != nil {
			return j.recordSegmentRecovery(ctx, segmentID, RecoveryRecord{
				ID: recoveryID(segmentID, offset, "header"), SegmentID: segmentID,
				Kind: RecoveryCorrupt, RecordOffset: offset, AvailableBytes: remaining,
				Detail: "record header failed integrity validation",
			})
		}
		recordLength, ok := checkedRecordLength(recordHeader)
		if !ok {
			return j.recordSegmentRecovery(ctx, segmentID, RecoveryRecord{
				ID: recoveryID(segmentID, offset, "length"), SegmentID: segmentID,
				Kind: RecoveryCorrupt, RecordOffset: offset, AvailableBytes: remaining,
				Detail: "record length exceeds implementation bounds",
			})
		}
		metadataAvailable := minInt64(int64(recordHeader.MetadataLength), maxInt64(0, remaining-recordHeaderSize))
		metadataBytes := make([]byte, metadataAvailable)
		if metadataAvailable > 0 {
			if _, err := file.ReadAt(metadataBytes, offset+recordHeaderSize); err != nil {
				return err
			}
		}
		metadata, metadataValid := validateRecordMetadata(recordHeader, metadataBytes)
		payloadOffset := offset + recordHeaderSize + int64(recordHeader.MetadataLength)
		if remaining < recordLength {
			availablePayload := maxInt64(0, info.Size()-payloadOffset)
			recovery := RecoveryRecord{
				ID: recoveryID(segmentID, offset, metadata.ExtentID), ExtentID: metadata.ExtentID,
				OperationID: metadata.OperationID, SegmentID: segmentID, Kind: RecoveryIncomplete,
				RecordOffset: offset, AvailableBytes: remaining, PayloadOffset: payloadOffset,
				PayloadLength: availablePayload, LogicalOffset: metadata.LogicalOffset,
				Detail: "record payload is incomplete",
			}
			if !metadataValid {
				recovery.ExtentID, recovery.OperationID = "", ""
				recovery.Detail = "record metadata or payload is incomplete"
			}
			return j.recordSegmentRecovery(ctx, segmentID, recovery)
		}
		if !metadataValid {
			if err := j.recordSegmentRecovery(ctx, segmentID, RecoveryRecord{
				ID: recoveryID(segmentID, offset, "metadata"), SegmentID: segmentID,
				Kind: RecoveryCorrupt, RecordOffset: offset, AvailableBytes: recordLength,
				PayloadOffset: payloadOffset, PayloadLength: int64(recordHeader.PayloadLength),
				Detail: "record metadata failed integrity validation",
			}); err != nil {
				return err
			}
			offset += recordLength
			continue
		}
		payloadValid, err := validateRecordPayload(file, payloadOffset, recordHeader)
		if err != nil {
			return err
		}
		if !payloadValid {
			if err := j.recordSegmentRecovery(ctx, segmentID, RecoveryRecord{
				ID: recoveryID(segmentID, offset, metadata.ExtentID), ExtentID: metadata.ExtentID,
				OperationID: metadata.OperationID, SegmentID: segmentID, Kind: RecoveryCorrupt,
				RecordOffset: offset, AvailableBytes: recordLength, PayloadOffset: payloadOffset,
				PayloadLength: int64(recordHeader.PayloadLength), LogicalOffset: metadata.LogicalOffset,
				CRC32: recordHeader.PayloadCRC32, SHA256: hex.EncodeToString(recordHeader.PayloadSHA256[:]),
				Detail: "record payload failed checksum validation",
			}); err != nil {
				return err
			}
			offset += recordLength
			continue
		}
		if extent, found := referenced[key]; found {
			if extent.ID != metadata.ExtentID || extent.OperationID != metadata.OperationID ||
				extent.LogicalOffset != metadata.LogicalOffset || extent.Length != int64(recordHeader.PayloadLength) ||
				extent.RecordLength != recordLength || extent.PayloadOffset != payloadOffset ||
				extent.CRC32 != recordHeader.PayloadCRC32 || extent.SHA256 != hex.EncodeToString(recordHeader.PayloadSHA256[:]) {
				if err := j.recordSegmentRecovery(ctx, segmentID, RecoveryRecord{
					ID: recoveryID(segmentID, offset, extent.ID), ExtentID: extent.ID,
					OperationID: extent.OperationID, SegmentID: segmentID, Kind: RecoveryMismatch,
					RecordOffset: offset, AvailableBytes: recordLength, PayloadOffset: payloadOffset,
					PayloadLength: int64(recordHeader.PayloadLength), LogicalOffset: metadata.LogicalOffset,
					CRC32: recordHeader.PayloadCRC32, SHA256: hex.EncodeToString(recordHeader.PayloadSHA256[:]),
					Detail: "segment record and committed extent metadata disagree",
				}); err != nil {
					return err
				}
			} else {
				seen[key] = struct{}{}
			}
		} else {
			if err := j.recordRecovery(ctx, RecoveryRecord{
				ID: recoveryID(segmentID, offset, metadata.ExtentID), ExtentID: metadata.ExtentID,
				OperationID: metadata.OperationID, SegmentID: segmentID, Kind: RecoveryOrphan,
				RecordOffset: offset, AvailableBytes: recordLength, PayloadOffset: payloadOffset,
				PayloadLength: int64(recordHeader.PayloadLength), LogicalOffset: metadata.LogicalOffset,
				CRC32: recordHeader.PayloadCRC32, SHA256: hex.EncodeToString(recordHeader.PayloadSHA256[:]),
				Detail: "durable segment record has no committed SQLite extent reference",
			}); err != nil {
				return err
			}
		}
		offset += recordLength
	}
	return nil
}

func validSegmentFileName(name string) bool {
	if len(name) != len("segment-")+32+len(".dat") || !strings.HasPrefix(name, "segment-") || !strings.HasSuffix(name, ".dat") {
		return false
	}
	id := strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".dat")
	decoded, err := hex.DecodeString(id)
	return err == nil && len(decoded) == 16
}

func validSegmentHeader(header []byte, expectedID string) bool {
	if int64(len(header)) != segmentHeaderSize || string(header[0:8]) != string(segmentMagic[:]) ||
		binary.LittleEndian.Uint32(header[8:12]) != segmentVersion ||
		binary.LittleEndian.Uint32(header[12:16]) != uint32(segmentHeaderSize) {
		return false
	}
	return hex.EncodeToString(header[16:32]) == expectedID
}

func checkedRecordLength(header decodedRecordHeader) (int64, bool) {
	if header.PayloadLength > uint64(^uint64(0)>>1) {
		return 0, false
	}
	length := recordHeaderSize + int64(header.MetadataLength)
	payload := int64(header.PayloadLength)
	if payload > int64(^uint64(0)>>1)-length {
		return 0, false
	}
	return length + payload, true
}

func validateRecordMetadata(header decodedRecordHeader, payload []byte) (segmentRecordMetadata, bool) {
	var metadata segmentRecordMetadata
	if len(payload) != int(header.MetadataLength) || crc32.ChecksumIEEE(payload) != header.MetadataCRC32 || sha256.Sum256(payload) != header.MetadataSHA256 {
		return metadata, false
	}
	if err := json.Unmarshal(payload, &metadata); err != nil || !validIdentifier(metadata.ExtentID) ||
		!validIdentifier(metadata.OperationID) || metadata.LogicalOffset < 0 {
		return segmentRecordMetadata{}, false
	}
	return metadata, true
}

func validateRecordPayload(file *os.File, offset int64, header decodedRecordHeader) (bool, error) {
	hash := sha256.New()
	checksum := crc32.NewIEEE()
	reader := io.NewSectionReader(file, offset, int64(header.PayloadLength))
	buffer := make([]byte, 1024*1024)
	written, err := io.CopyBuffer(io.MultiWriter(hash, checksum), reader, buffer)
	if err != nil {
		return false, err
	}
	return written == int64(header.PayloadLength) && checksum.Sum32() == header.PayloadCRC32 &&
		hex.EncodeToString(hash.Sum(nil)) == hex.EncodeToString(header.PayloadSHA256[:]), nil
}

func (j *Journal) upsertSegment(ctx context.Context, id string, fileName string, fallbackState string, observedSize int64, now int64) error {
	_, err := j.db.ExecContext(ctx, `INSERT INTO segments (id, file_name, state, observed_size, created_at, updated_at)
 VALUES (?, ?, ?, ?, ?, ?)
 ON CONFLICT(id) DO UPDATE SET
 observed_size = excluded.observed_size,
 updated_at = excluded.updated_at,
 state = CASE
   WHEN segments.state = 'corrupt' THEN segments.state
   WHEN segments.state IN ('active','sealed') AND excluded.state = 'sealed' THEN segments.state
   ELSE excluded.state
 END;`,
		id, fileName, fallbackState, observedSize, now, now)
	return err
}

func (j *Journal) recordSegmentRecovery(ctx context.Context, segmentID string, recovery RecoveryRecord) error {
	now := j.options.Now().UnixNano()
	if _, err := j.db.ExecContext(ctx, `UPDATE segments SET state = 'corrupt', updated_at = ? WHERE id = ?;`, now, segmentID); err != nil {
		return err
	}
	return j.recordRecovery(ctx, recovery)
}

func recoveryID(segmentID string, offset int64, extentID string) string {
	payload := fmt.Sprintf("%s\x00%d\x00%s", segmentID, offset, extentID)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func segmentRecordKey(segmentID string, offset int64) string {
	return fmt.Sprintf("%s:%d", segmentID, offset)
}

func (j *Journal) recordRecovery(ctx context.Context, recovery RecoveryRecord) error {
	now := j.options.Now().UnixNano()
	if recovery.CreatedAt == 0 {
		recovery.CreatedAt = now
	}
	recovery.UpdatedAt = now
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO recovery_records (
 id, extent_id, operation_id, segment_id, kind, record_offset, available_bytes,
 payload_offset, payload_length, logical_offset, crc32, sha256, detail, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
 ON CONFLICT(id) DO UPDATE SET
 extent_id = excluded.extent_id,
 operation_id = excluded.operation_id,
 segment_id = excluded.segment_id,
 kind = CASE WHEN recovery_records.kind = 'restored' THEN recovery_records.kind ELSE excluded.kind END,
 record_offset = excluded.record_offset,
 available_bytes = excluded.available_bytes,
 payload_offset = excluded.payload_offset,
 payload_length = excluded.payload_length,
 logical_offset = excluded.logical_offset,
 crc32 = excluded.crc32,
 sha256 = excluded.sha256,
 detail = CASE WHEN recovery_records.kind = 'restored' THEN recovery_records.detail ELSE excluded.detail END,
 updated_at = excluded.updated_at;`,
		recovery.ID, recovery.ExtentID, recovery.OperationID, recovery.SegmentID,
		recovery.Kind, recovery.RecordOffset, recovery.AvailableBytes,
		recovery.PayloadOffset, recovery.PayloadLength, recovery.LogicalOffset,
		int64(recovery.CRC32), recovery.SHA256, recovery.Detail,
		recovery.CreatedAt, recovery.UpdatedAt)
	if err != nil {
		return err
	}
	if recovery.OperationID != "" {
		result, err := tx.ExecContext(ctx, `UPDATE operations SET state = 'recovery', last_error = ?, updated_at = ?
 WHERE id = ? AND state NOT IN ('remote_committed','abandoned','recovery');`, recovery.Detail, now, recovery.OperationID)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed > 0 {
			if err := appendEvent(ctx, tx, recovery.OperationID, "recovery_detected", recovery.Kind, recovery.ID, now); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func recoveryExistsForExtent(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, extentID string) (bool, error) {
	var count int
	err := query.QueryRowContext(ctx, `SELECT COUNT(*) FROM recovery_records WHERE extent_id = ? AND kind <> 'restored';`, extentID).Scan(&count)
	return count > 0, err
}

func (j *Journal) ListRecovery(ctx context.Context) ([]RecoveryRecord, error) {
	return listRecovery(ctx, j.db)
}

func (i *Inspector) ListRecovery(ctx context.Context) ([]RecoveryRecord, error) {
	return listRecovery(ctx, i.db)
}

func listRecovery(ctx context.Context, db *sql.DB) ([]RecoveryRecord, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, extent_id, operation_id, segment_id, kind,
 record_offset, available_bytes, payload_offset, payload_length, logical_offset,
 crc32, sha256, detail, created_at, updated_at
 FROM recovery_records ORDER BY created_at ASC, id ASC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]RecoveryRecord, 0)
	for rows.Next() {
		recovery, err := scanRecovery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, recovery)
	}
	return result, rows.Err()
}

func scanRecovery(row rowScanner) (RecoveryRecord, error) {
	var recovery RecoveryRecord
	var checksum int64
	err := row.Scan(
		&recovery.ID, &recovery.ExtentID, &recovery.OperationID, &recovery.SegmentID,
		&recovery.Kind, &recovery.RecordOffset, &recovery.AvailableBytes,
		&recovery.PayloadOffset, &recovery.PayloadLength, &recovery.LogicalOffset,
		&checksum, &recovery.SHA256, &recovery.Detail,
		&recovery.CreatedAt, &recovery.UpdatedAt,
	)
	recovery.CRC32 = uint32(checksum)
	return recovery, err
}

func getRecovery(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id string) (RecoveryRecord, error) {
	return scanRecovery(query.QueryRowContext(ctx, `SELECT id, extent_id, operation_id, segment_id, kind,
 record_offset, available_bytes, payload_offset, payload_length, logical_offset,
 crc32, sha256, detail, created_at, updated_at
 FROM recovery_records WHERE id = ?;`, strings.TrimSpace(id)))
}

func (j *Journal) ExportRecoveryPayload(ctx context.Context, recoveryID string, destination string) (RecoveryRecord, error) {
	return exportRecoveryPayload(ctx, j.dir, j.db, recoveryID, destination)
}

func (i *Inspector) ExportRecoveryPayload(ctx context.Context, recoveryID string, destination string) (RecoveryRecord, error) {
	return exportRecoveryPayload(ctx, i.dir, i.db, recoveryID, destination)
}

func exportRecoveryPayload(ctx context.Context, dir string, db *sql.DB, recoveryID string, destination string) (RecoveryRecord, error) {
	recovery, err := getRecovery(ctx, db, recoveryID)
	if err != nil {
		return RecoveryRecord{}, err
	}
	if recovery.SegmentID == "" || recovery.PayloadLength <= 0 || recovery.PayloadOffset < segmentHeaderSize {
		return RecoveryRecord{}, errors.New("recovery record has no exportable payload bytes")
	}
	segmentPath, err := segmentPathForID(dir, db, recovery.SegmentID)
	if err != nil {
		return RecoveryRecord{}, err
	}
	source, err := os.Open(segmentPath)
	if err != nil {
		return RecoveryRecord{}, err
	}
	defer source.Close()
	destination = strings.TrimSpace(destination)
	if destination == "" {
		return RecoveryRecord{}, errors.New("recovery export destination is required")
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return RecoveryRecord{}, err
	}
	reader := io.NewSectionReader(source, recovery.PayloadOffset, recovery.PayloadLength)
	written, copyErr := io.Copy(output, reader)
	if copyErr == nil && written != recovery.PayloadLength {
		copyErr = io.ErrUnexpectedEOF
	}
	if copyErr == nil {
		copyErr = output.Sync()
	}
	closeErr := output.Close()
	if copyErr == nil {
		copyErr = closeErr
	}
	if copyErr == nil {
		copyErr = syncJournalDirectory(filepath.Dir(destination))
	}
	if copyErr != nil {
		return RecoveryRecord{}, fmt.Errorf("recovery export left an incomplete exclusive destination for manual inspection: %w", copyErr)
	}
	return recovery, nil
}

func (j *Journal) RestoreRecovery(ctx context.Context, recoveryID string) (Extent, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	recovery, err := getRecovery(ctx, j.db, recoveryID)
	if err != nil {
		return Extent{}, err
	}
	if recovery.Kind == RecoveryRestored {
		return getExtent(ctx, j.db, recovery.ExtentID)
	}
	if recovery.Kind != RecoveryOrphan || !validIdentifier(recovery.ExtentID) || !validIdentifier(recovery.OperationID) || recovery.PayloadLength <= 0 {
		return Extent{}, ErrRecoveryRequired
	}
	path, err := segmentPathForID(j.dir, j.db, recovery.SegmentID)
	if err != nil {
		return Extent{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return Extent{}, err
	}
	extent := Extent{
		ID: recovery.ExtentID, OperationID: recovery.OperationID, Kind: ExtentData,
		LogicalOffset: recovery.LogicalOffset, Length: recovery.PayloadLength,
		SegmentID: recovery.SegmentID, RecordOffset: recovery.RecordOffset,
		RecordLength: recovery.AvailableBytes, PayloadOffset: recovery.PayloadOffset,
		CRC32: recovery.CRC32, SHA256: recovery.SHA256,
	}
	verifyErr := verifyExtentPayload(file, extent)
	_ = file.Close()
	if verifyErr != nil {
		return Extent{}, verifyErr
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return Extent{}, err
	}
	defer tx.Rollback()
	operation, err := getOperation(ctx, tx, recovery.OperationID)
	if err != nil || recovery.LogicalOffset+recovery.PayloadLength > operation.NewSize {
		return Extent{}, ErrRecoveryRequired
	}
	if existing, err := getExtent(ctx, tx, recovery.ExtentID); err == nil {
		if existing.SHA256 != recovery.SHA256 || existing.OperationID != recovery.OperationID {
			return Extent{}, ErrIdempotencyReuse
		}
		now := j.options.Now().UnixNano()
		if _, err := tx.ExecContext(ctx, `UPDATE recovery_records SET kind = 'restored', detail = 'restored into committed extent metadata', updated_at = ? WHERE id = ?;`, now, recovery.ID); err != nil {
			return Extent{}, err
		}
		if err := tx.Commit(); err != nil {
			return Extent{}, err
		}
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Extent{}, err
	}
	sequence, err := nextExtentSequence(ctx, tx, recovery.OperationID)
	if err != nil {
		return Extent{}, err
	}
	now := j.options.Now().UnixNano()
	extent.Sequence, extent.CreatedAt = sequence, now
	_, err = tx.ExecContext(ctx, `INSERT INTO extents (
 id, operation_id, sequence, kind, logical_offset, length, segment_id,
 record_offset, record_length, payload_offset, crc32, sha256, created_at
) VALUES (?, ?, ?, 'data', ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		extent.ID, extent.OperationID, extent.Sequence, extent.LogicalOffset,
		extent.Length, extent.SegmentID, extent.RecordOffset, extent.RecordLength,
		extent.PayloadOffset, int64(extent.CRC32), extent.SHA256, extent.CreatedAt)
	if err != nil {
		return Extent{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE recovery_records SET kind = 'restored', detail = 'restored into committed extent metadata', updated_at = ? WHERE id = ? AND kind = 'orphan';`, now, recovery.ID); err != nil {
		return Extent{}, err
	}
	if err := markOperationLocallyDurable(ctx, tx, operation.ID, operation.State, now); err != nil {
		return Extent{}, err
	}
	if err := appendEvent(ctx, tx, operation.ID, "recovery_restore", StateLocalDurable, recovery.ID, now); err != nil {
		return Extent{}, err
	}
	if err := tx.Commit(); err != nil {
		return Extent{}, err
	}
	return extent, nil
}

func (j *Journal) Stats(ctx context.Context) (Stats, error) {
	return journalStats(ctx, j.dir, j.db)
}

func (i *Inspector) Stats(ctx context.Context) (Stats, error) {
	return journalStats(ctx, i.dir, i.db)
}

func journalStats(ctx context.Context, dir string, db *sql.DB) (Stats, error) {
	var stats Stats
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*), COALESCE(SUM(CASE WHEN state NOT IN ('remote_committed','abandoned') THEN 1 ELSE 0 END), 0) FROM operations;`).Scan(&stats.Operations, &stats.PendingOperations); err != nil {
		return Stats{}, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM recovery_records WHERE kind <> 'restored';`).Scan(&stats.RecoveryRecords); err != nil {
		return Stats{}, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(SUM(e.length), 0)
 FROM extents e JOIN operations o ON o.id = e.operation_id
 WHERE e.kind = 'data' AND o.state NOT IN ('remote_committed','abandoned');`).Scan(&stats.DirtyBytes); err != nil {
		return Stats{}, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return Stats{}, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && validSegmentFileName(entry.Name()) {
			info, err := entry.Info()
			if err != nil {
				return Stats{}, err
			}
			stats.SegmentBytes += info.Size()
		}
	}
	return stats, nil
}
