package protocol

type UploadPrepareRequest struct {
	Name    string                      `json:"name"`
	Size    int64                       `json:"size,omitempty"`
	Entries []UploadArchiveSizeManifest `json:"entries,omitempty"`
}

type UploadArchiveSizeManifest struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

type UploadPrepareResponse struct {
	URL             string              `json:"url"`
	UploadPath      string              `json:"uploadPath"`
	FileID          string              `json:"fileID"`
	FileName        string              `json:"fileName"`
	ChunkSize       int64               `json:"chunkSize"`
	FinalizeURL     string              `json:"finalizeUrl"`
	AssignmentMode  string              `json:"assignmentMode"`
	TargetSchedule  []int               `json:"targetSchedule,omitempty"`
	CommittedChunks []int64             `json:"committedChunks,omitempty"`
	CommittedBytes  int64               `json:"committedBytes,omitempty"`
	Nodes           []UploadPrepareNode `json:"nodes"`
	FallbackNodes   []UploadPrepareNode `json:"fallbackNodes,omitempty"`
}

type UploadPrepareNode struct {
	ID               string `json:"id"`
	PublicURL        string `json:"publicUrl"`
	Weight           int64  `json:"weight"`
	MaxParallel      int    `json:"maxParallel,omitempty"`
	AssignmentMode   string `json:"assignmentMode,omitempty"`
	FailoverPriority int    `json:"failoverPriority,omitempty"`
}
