package cli

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

const (
	initialFileSniffSize = 512
	maximumFileSniffSize = 64 * 1024
	tarHeaderSize        = 512
)

// sniffFileExtension reads the smallest useful prefix and returns it so a
// non-seekable caller can replay every inspected byte. Most formats need only
// the first 512 bytes. Compressed streams are extended incrementally, up to a
// small fixed ceiling, only when identifying an inner tar stream needs more.
func sniffFileExtension(reader io.Reader) ([]byte, string, error) {
	if reader == nil {
		return nil, ".bin", errors.New("file type reader is unavailable")
	}

	prefix := make([]byte, 0, initialFileSniffSize)
	target := initialFileSniffSize
	complete := false

	for {
		if len(prefix) < target && !complete {
			if cap(prefix) < target {
				grown := make([]byte, len(prefix), target)
				copy(grown, prefix)
				prefix = grown
			}
			start := len(prefix)
			prefix = prefix[:target]
			n, err := io.ReadFull(reader, prefix[start:target])
			prefix = prefix[:start+n]
			switch {
			case err == nil:
			case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
				complete = true
			default:
				return prefix, "", err
			}
		}

		extension, needMore := extensionFromPrefix(prefix, complete)
		if !needMore || complete || target >= maximumFileSniffSize {
			if extension == "" {
				extension = ".bin"
			}
			return prefix, extension, nil
		}

		target *= 2
		if target > maximumFileSniffSize {
			target = maximumFileSniffSize
		}
	}
}

func filenameHasExtension(name string) bool {
	name = strings.TrimSpace(name)
	dot := strings.LastIndexByte(name, '.')
	return dot >= 0 && dot < len(name)-1
}

func appendDetectedExtension(name, extension string) string {
	name = strings.TrimRight(strings.TrimSpace(name), ". ")
	if name == "" {
		name = "unnamed-file"
	}
	if filenameHasExtension(name) {
		return name
	}
	if extension == "" {
		extension = ".bin"
	}
	if !strings.HasPrefix(extension, ".") {
		extension = "." + extension
	}
	return name + extension
}

func extensionFromPrefix(data []byte, complete bool) (string, bool) {
	if len(data) == 0 {
		if complete {
			return ".bin", false
		}
		return "", true
	}

	switch {
	case bytes.HasPrefix(data, []byte{0x1f, 0x8b}):
		isTar, needMore := gzipPrefixIsTar(data, complete)
		if isTar {
			return ".tar.gz", false
		}
		return ".gz", needMore
	case bytes.HasPrefix(data, []byte("BZh")):
		isTar, needMore := bzip2PrefixIsTar(data, complete)
		if isTar {
			return ".tar.bz2", false
		}
		return ".bz2", needMore
	case isLZ4Magic(data):
		isTar, needMore := lz4PrefixIsTar(data, complete)
		if isTar {
			return ".tar.lz4", false
		}
		return ".lz4", needMore
	}

	if len(data) >= tarHeaderSize && isTarHeader(data[:tarHeaderSize]) {
		return ".tar", false
	}
	if extension := magicFileExtension(data); extension != "" {
		return extension, false
	}
	if !complete && len(data) < initialFileSniffSize {
		return "", true
	}
	return detectedContentExtension(data, complete), false
}

func gzipPrefixIsTar(data []byte, complete bool) (bool, bool) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return false, !complete
	}
	defer reader.Close()
	return decompressedPrefixIsTar(reader, complete)
}

func bzip2PrefixIsTar(data []byte, complete bool) (bool, bool) {
	return decompressedPrefixIsTar(bzip2.NewReader(bytes.NewReader(data)), complete)
}

func decompressedPrefixIsTar(reader io.Reader, complete bool) (bool, bool) {
	header := make([]byte, tarHeaderSize)
	read := 0
	for read < len(header) {
		n, err := reader.Read(header[read:])
		read += n
		if read == len(header) {
			return isTarHeader(header), false
		}
		if errors.Is(err, io.EOF) {
			// A clean decoder EOF proves this is a complete short payload, even
			// if the underlying stdin pipe itself has not closed yet.
			return false, false
		}
		if err != nil {
			if complete {
				return false, false
			}
			// Unexpected EOF and checksum errors commonly mean the compressed
			// source prefix ended mid-block, so inspect another small increment.
			return false, true
		}
		if n == 0 {
			return false, !complete
		}
	}
	return isTarHeader(header), false
}

func isTarHeader(header []byte) bool {
	if len(header) < tarHeaderSize {
		return false
	}
	header = header[:tarHeaderSize]
	allZero := true
	for _, value := range header {
		if value != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return false
	}

	stored, ok := parseTarChecksum(header[148:156])
	if !ok {
		return false
	}
	unsignedSum := 0
	signedSum := 0
	for index, value := range header {
		if index >= 148 && index < 156 {
			value = ' '
		}
		unsignedSum += int(value)
		signedSum += int(int8(value))
	}
	return stored == unsignedSum || stored == signedSum
}

func parseTarChecksum(field []byte) (int, bool) {
	field = bytes.Trim(field, " \x00")
	if len(field) == 0 {
		return 0, false
	}
	value := 0
	for _, digit := range field {
		if digit < '0' || digit > '7' {
			return 0, false
		}
		value = value*8 + int(digit-'0')
	}
	return value, true
}

func magicFileExtension(data []byte) string {
	hasPrefix := func(values ...byte) bool {
		return bytes.HasPrefix(data, values)
	}

	switch {
	case hasPrefix(0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00):
		return ".xz"
	case hasPrefix(0x28, 0xb5, 0x2f, 0xfd):
		return ".zst"
	case hasPrefix(0x37, 0x7a, 0xbc, 0xaf, 0x27, 0x1c):
		return ".7z"
	case bytes.HasPrefix(data, []byte("Rar!\x1a\x07")):
		return ".rar"
	case bytes.HasPrefix(data, []byte("PK\x03\x04")),
		bytes.HasPrefix(data, []byte("PK\x05\x06")),
		bytes.HasPrefix(data, []byte("PK\x07\x08")):
		return zipContainerExtension(data)
	case bytes.HasPrefix(data, []byte("%PDF-")):
		return ".pdf"
	case hasPrefix(0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a):
		return ".png"
	case len(data) >= 3 && data[0] == 0xff && data[1] == 0xd8 && data[2] == 0xff:
		return ".jpg"
	case bytes.HasPrefix(data, []byte("GIF87a")), bytes.HasPrefix(data, []byte("GIF89a")):
		return ".gif"
	case bytes.HasPrefix(data, []byte("II*\x00")), bytes.HasPrefix(data, []byte("MM\x00*")):
		return ".tiff"
	case bytes.HasPrefix(data, []byte("BM")):
		return ".bmp"
	case hasPrefix(0x00, 0x00, 0x01, 0x00):
		return ".ico"
	case bytes.HasPrefix(data, []byte("8BPS")):
		return ".psd"
	case hasPrefix(0x76, 0x2f, 0x31, 0x01):
		return ".exr"
	case bytes.HasPrefix(data, []byte("qoif")):
		return ".qoi"
	case len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) && bytes.Equal(data[8:12], []byte("WEBP")):
		return ".webp"
	case len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) && bytes.Equal(data[8:12], []byte("WAVE")):
		return ".wav"
	case len(data) >= 12 && bytes.Equal(data[:4], []byte("RIFF")) && bytes.Equal(data[8:12], []byte("AVI ")):
		return ".avi"
	case len(data) >= 12 && bytes.Equal(data[4:8], []byte("ftyp")):
		return isoBaseMediaExtension(data[8:12])
	case hasPrefix(0x1a, 0x45, 0xdf, 0xa3):
		if bytes.Contains(bytes.ToLower(data), []byte("webm")) {
			return ".webm"
		}
		return ".mkv"
	case bytes.HasPrefix(data, []byte("OggS")):
		return ".ogg"
	case bytes.HasPrefix(data, []byte("fLaC")):
		return ".flac"
	case bytes.HasPrefix(data, []byte("MThd")):
		return ".mid"
	case bytes.HasPrefix(data, []byte("ID3")), looksLikeMPEGMusicFrame(data):
		return ".mp3"
	case hasPrefix(0x7f, 'E', 'L', 'F'):
		return ".elf"
	case looksLikePEExecutable(data):
		return ".exe"
	case isMachOMagic(data):
		return ".macho"
	case hasPrefix(0x00, 0x61, 0x73, 0x6d):
		return ".wasm"
	case hasPrefix(0xca, 0xfe, 0xba, 0xbe):
		return ".class"
	case bytes.HasPrefix(data, []byte("dex\n")):
		return ".dex"
	case bytes.HasPrefix(data, []byte("SQLite format 3\x00")):
		return ".sqlite3"
	case bytes.HasPrefix(data, []byte("{\\rtf")):
		return ".rtf"
	case bytes.HasPrefix(data, []byte("%!PS")):
		return ".ps"
	case bytes.HasPrefix(data, []byte("wOFF")):
		return ".woff"
	case bytes.HasPrefix(data, []byte("wOF2")):
		return ".woff2"
	case bytes.HasPrefix(data, []byte("OTTO")):
		return ".otf"
	case hasPrefix(0x00, 0x01, 0x00, 0x00):
		return ".ttf"
	case hasPrefix(0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1):
		return ".ole"
	default:
		return ""
	}
}

func zipContainerExtension(data []byte) string {
	lower := bytes.ToLower(data)
	switch {
	case bytes.Contains(lower, []byte("application/epub+zip")):
		return ".epub"
	case bytes.Contains(lower, []byte("androidmanifest.xml")):
		return ".apk"
	case bytes.Contains(lower, []byte("meta-inf/manifest.mf")):
		return ".jar"
	case bytes.Contains(lower, []byte("word/")):
		return ".docx"
	case bytes.Contains(lower, []byte("xl/")):
		return ".xlsx"
	case bytes.Contains(lower, []byte("ppt/")):
		return ".pptx"
	case bytes.Contains(lower, []byte("application/vnd.oasis.opendocument.text")):
		return ".odt"
	case bytes.Contains(lower, []byte("application/vnd.oasis.opendocument.spreadsheet")):
		return ".ods"
	case bytes.Contains(lower, []byte("application/vnd.oasis.opendocument.presentation")):
		return ".odp"
	default:
		return ".zip"
	}
}

func isoBaseMediaExtension(brand []byte) string {
	switch strings.ToLower(string(brand)) {
	case "avif", "avis":
		return ".avif"
	case "heic", "heix", "hevc", "hevx":
		return ".heic"
	case "mif1", "msf1":
		return ".heif"
	case "qt  ":
		return ".mov"
	case "m4a ", "m4p ":
		return ".m4a"
	case "m4b ":
		return ".m4b"
	case "3gp4", "3gp5", "3gp6", "3gp7", "3gp8", "3gp9":
		return ".3gp"
	default:
		return ".mp4"
	}
}

func looksLikeMPEGMusicFrame(data []byte) bool {
	if len(data) < 4 || data[0] != 0xff || data[1]&0xe0 != 0xe0 {
		return false
	}
	version := (data[1] >> 3) & 0x03
	layer := (data[1] >> 1) & 0x03
	bitrate := (data[2] >> 4) & 0x0f
	sampleRate := (data[2] >> 2) & 0x03
	return version != 1 && layer != 0 && bitrate != 0 && bitrate != 0x0f && sampleRate != 3
}

func looksLikePEExecutable(data []byte) bool {
	if len(data) < 0x40 || data[0] != 'M' || data[1] != 'Z' {
		return false
	}
	offset := int(binary.LittleEndian.Uint32(data[0x3c:0x40]))
	return offset >= 0 && offset+4 <= len(data) && bytes.Equal(data[offset:offset+4], []byte("PE\x00\x00"))
}

func isMachOMagic(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	magic := binary.BigEndian.Uint32(data[:4])
	switch magic {
	case 0xfeedface, 0xfeedfacf, 0xcefaedfe, 0xcffaedfe:
		return true
	case 0xcafebabe:
		if len(data) < 8 {
			return false
		}
		architectures := binary.BigEndian.Uint32(data[4:8])
		return architectures > 0 && architectures <= 32
	case 0xbebafeca:
		if len(data) < 8 {
			return false
		}
		architectures := binary.LittleEndian.Uint32(data[4:8])
		return architectures > 0 && architectures <= 32
	default:
		return false
	}
}

func detectedContentExtension(data []byte, complete bool) string {
	trimmed := bytes.TrimSpace(bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf}))
	lower := bytes.ToLower(trimmed)
	switch {
	case complete && json.Valid(trimmed):
		return ".json"
	case bytes.HasPrefix(lower, []byte("<!doctype html")), bytes.HasPrefix(lower, []byte("<html")):
		return ".html"
	case bytes.HasPrefix(lower, []byte("<svg")),
		bytes.HasPrefix(lower, []byte("<?xml")) && bytes.Contains(lower, []byte("<svg")):
		return ".svg"
	case bytes.HasPrefix(lower, []byte("<?xml")):
		return ".xml"
	}

	contentType := strings.ToLower(strings.TrimSpace(strings.SplitN(http.DetectContentType(data), ";", 2)[0]))
	switch contentType {
	case "text/html":
		return ".html"
	case "text/xml", "application/xml":
		return ".xml"
	case "text/plain":
		return ".txt"
	case "image/svg+xml":
		return ".svg"
	case "application/json":
		return ".json"
	case "application/pdf":
		return ".pdf"
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "audio/mpeg":
		return ".mp3"
	case "audio/wave", "audio/wav", "audio/x-wav":
		return ".wav"
	case "application/ogg":
		return ".ogg"
	default:
		return ".bin"
	}
}

func isLZ4Magic(data []byte) bool {
	return bytes.HasPrefix(data, []byte{0x04, 0x22, 0x4d, 0x18}) ||
		bytes.HasPrefix(data, []byte{0x02, 0x21, 0x4c, 0x18})
}

func lz4PrefixIsTar(data []byte, complete bool) (bool, bool) {
	decoded, needMore, valid := decodeLZ4FramePrefix(data, tarHeaderSize, complete)
	if !valid {
		return false, false
	}
	if len(decoded) >= tarHeaderSize {
		return isTarHeader(decoded[:tarHeaderSize]), false
	}
	return false, needMore
}

func decodeLZ4FramePrefix(data []byte, want int, complete bool) ([]byte, bool, bool) {
	if len(data) < 4 {
		return nil, !complete, true
	}
	if bytes.HasPrefix(data, []byte{0x02, 0x21, 0x4c, 0x18}) {
		return decodeLZ4BlocksPrefix(data[4:], want, 8*1024*1024, true, false, complete)
	}
	if !bytes.HasPrefix(data, []byte{0x04, 0x22, 0x4d, 0x18}) {
		return nil, false, false
	}
	if len(data) < 7 {
		return nil, !complete, true
	}

	flags := data[4]
	blockDescriptor := data[5]
	if flags>>6 != 1 || flags&0x02 != 0 {
		return nil, false, false
	}
	maximumBlockSize := map[byte]int{4: 64 * 1024, 5: 256 * 1024, 6: 1024 * 1024, 7: 4 * 1024 * 1024}[(blockDescriptor>>4)&0x07]
	if maximumBlockSize == 0 {
		return nil, false, false
	}

	offset := 6
	if flags&0x08 != 0 {
		offset += 8
	}
	hasDictionary := flags&0x01 != 0
	if hasDictionary {
		offset += 4
	}
	offset++ // descriptor checksum
	if len(data) < offset {
		return nil, !complete, true
	}
	if hasDictionary {
		// The external dictionary is not present in the stream prefix, so the
		// outer LZ4 type is certain but inspecting its payload is not.
		return nil, false, true
	}

	independentBlocks := flags&0x20 != 0
	blockChecksums := flags&0x10 != 0
	return decodeLZ4BlocksPrefix(data[offset:], want, maximumBlockSize, independentBlocks, blockChecksums, complete)
}

func decodeLZ4BlocksPrefix(data []byte, want, maximumBlockSize int, independent, checksummed, complete bool) ([]byte, bool, bool) {
	decoded := make([]byte, 0, want)
	offset := 0
	for len(decoded) < want {
		if len(data)-offset < 4 {
			return decoded, !complete, len(data) == offset || !complete
		}
		blockWord := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
		if blockWord == 0 {
			return decoded, false, true
		}
		uncompressed := blockWord&0x80000000 != 0
		blockSize := int(blockWord & 0x7fffffff)
		if blockSize <= 0 || blockSize > maximumBlockSize {
			return decoded, false, false
		}

		available := len(data) - offset
		blockComplete := available >= blockSize
		if available > blockSize {
			available = blockSize
		}
		blockData := data[offset : offset+available]
		blockHistoryFloor := 0
		if independent {
			blockHistoryFloor = len(decoded)
		}

		if uncompressed {
			needed := want - len(decoded)
			if available > needed {
				available = needed
			}
			decoded = append(decoded, blockData[:available]...)
		} else {
			var needMore, valid bool
			decoded, needMore, valid = decodeLZ4BlockPrefix(blockData, blockComplete, decoded, want, blockHistoryFloor)
			if !valid {
				return decoded, false, false
			}
			if needMore {
				return decoded, !complete, true
			}
		}
		if len(decoded) >= want {
			return decoded, false, true
		}
		if !blockComplete {
			return decoded, !complete, !complete
		}
		offset += blockSize
		if checksummed {
			if len(data)-offset < 4 {
				return decoded, !complete, !complete
			}
			offset += 4
		}
	}
	return decoded, false, true
}

func decodeLZ4BlockPrefix(src []byte, srcComplete bool, decoded []byte, want, historyFloor int) ([]byte, bool, bool) {
	offset := 0
	for len(decoded) < want {
		if offset >= len(src) {
			if srcComplete {
				return decoded, false, true
			}
			return decoded, true, true
		}
		token := src[offset]
		offset++

		literalLength, needMore, valid := decodeLZ4Length(src, &offset, int(token>>4), srcComplete)
		if !valid || needMore {
			return decoded, needMore, valid
		}
		availableLiterals := len(src) - offset
		copyLiterals := literalLength
		if copyLiterals > availableLiterals {
			copyLiterals = availableLiterals
		}
		if copyLiterals > want-len(decoded) {
			copyLiterals = want - len(decoded)
		}
		decoded = append(decoded, src[offset:offset+copyLiterals]...)
		if len(decoded) >= want {
			return decoded, false, true
		}
		if availableLiterals < literalLength {
			return decoded, !srcComplete, !srcComplete
		}
		offset += literalLength
		if offset == len(src) {
			if srcComplete {
				return decoded, false, true
			}
			return decoded, true, true
		}
		if len(src)-offset < 2 {
			return decoded, !srcComplete, !srcComplete
		}
		matchOffset := int(binary.LittleEndian.Uint16(src[offset : offset+2]))
		offset += 2
		if matchOffset <= 0 || matchOffset > len(decoded)-historyFloor {
			return decoded, false, false
		}

		matchLength, needMore, valid := decodeLZ4Length(src, &offset, int(token&0x0f), srcComplete)
		if !valid || needMore {
			return decoded, needMore, valid
		}
		matchLength += 4
		if matchLength > want-len(decoded) {
			matchLength = want - len(decoded)
		}
		for index := 0; index < matchLength; index++ {
			decoded = append(decoded, decoded[len(decoded)-matchOffset])
		}
	}
	return decoded, false, true
}

func decodeLZ4Length(src []byte, offset *int, base int, complete bool) (int, bool, bool) {
	length := base
	if base != 15 {
		return length, false, true
	}
	for {
		if *offset >= len(src) {
			return length, !complete, !complete
		}
		value := int(src[*offset])
		(*offset)++
		if length > int(^uint(0)>>1)-value {
			return 0, false, false
		}
		length += value
		if value != 255 {
			return length, false, true
		}
	}
}
