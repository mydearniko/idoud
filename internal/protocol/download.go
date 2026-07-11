package protocol

type DownloadPlan struct {
	Version        string           `json:"version"`
	FileID         string           `json:"fileId"`
	FileName       string           `json:"fileName"`
	Size           int64            `json:"size"`
	ChunkSize      int64            `json:"chunkSize"`
	ChunkCount     int64            `json:"chunkCount"`
	PublicURL      string           `json:"publicUrl"`
	DownloadURL    string           `json:"downloadUrl"`
	ETag           string           `json:"etag"`
	AcceptRanges   string           `json:"acceptRanges"`
	AssignmentMode string           `json:"assignmentMode"`
	Mirrors        []DownloadMirror `json:"mirrors"`
	Ranges         []DownloadRange  `json:"ranges"`
}

type DownloadMirror struct {
	Index            int    `json:"index"`
	NodeID           string `json:"nodeId,omitempty"`
	URL              string `json:"url"`
	Weight           int64  `json:"weight"`
	MaxParallel      int    `json:"maxParallel"`
	SupportsRange    bool   `json:"supportsRange"`
	FailoverPriority int    `json:"failoverPriority,omitempty"`
}

type DownloadRange struct {
	Index         int   `json:"index"`
	Offset        int64 `json:"offset"`
	End           int64 `json:"end"`
	Size          int64 `json:"size"`
	PrimaryMirror int   `json:"primaryMirror"`
	MirrorIndexes []int `json:"mirrorIndexes,omitempty"`
}
