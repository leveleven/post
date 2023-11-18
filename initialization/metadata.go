package initialization

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spacemeshos/post/shared"
)

const metadataFileName = "postdata_metadata.json"

func SaveMetadata(dir string, v *shared.PostMetadata) error {
	err := os.MkdirAll(dir, shared.OwnerReadWriteExec)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("dir creation failure: %w", err)
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("serialization failure: %w", err)
	}

	metadata_file, err := os.OpenFile(filepath.Join(dir, metadataFileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open metadata file failure: %w", err)
	}
	defer metadata_file.Close()

	_, err = metadata_file.Write(data)
	// err = os.WriteFile(filepath.Join(dir, metadataFileName), data, shared.OwnerReadWrite)
	if err != nil {
		return fmt.Errorf("write to disk failure: %w", err)
	}

	return nil
}

func LoadMetadata(dir string) (*shared.PostMetadata, error) {
	filename := filepath.Join(dir, metadataFileName)
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrStateMetadataFileMissing
		}
		return nil, fmt.Errorf("read file failure: %w", err)
	}

	metadata := shared.PostMetadata{}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}
