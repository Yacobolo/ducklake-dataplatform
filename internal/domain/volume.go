package domain

import "time"

// VolumeType identifies the type of volume.
const (
	VolumeTypeManaged  = "MANAGED"
	VolumeTypeExternal = "EXTERNAL"
)

// Volume represents a governed storage container for unstructured files.
type Volume struct {
	ID              string
	Name            string
	SchemaName      string
	CatalogName     string
	VolumeType      string // "MANAGED" or "EXTERNAL"
	StorageLocation string // S3/Azure/GCS URL
	Comment         string
	Owner           string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// CreateVolumeRequest holds parameters for creating a volume.
type CreateVolumeRequest struct {
	Name            string
	VolumeType      string
	StorageLocation string // required for EXTERNAL, auto-generated for MANAGED
	Comment         string
}

// UpdateVolumeRequest holds parameters for updating a volume.
type UpdateVolumeRequest struct {
	NewName *string
	Comment *string
	Owner   *string
}

// Validate checks that the request is well-formed.
func (r *CreateVolumeRequest) Validate() error {
	return ValidateCreateVolumeRequest(*r)
}

// ValidateCreateVolumeRequest validates a create-volume request.
func ValidateCreateVolumeRequest(req CreateVolumeRequest) error {
	if req.Name == "" {
		return ErrValidation("volume name is required")
	}
	if len(req.Name) > 128 {
		return ErrValidation("volume name must be at most 128 characters")
	}
	switch req.VolumeType {
	case VolumeTypeManaged, VolumeTypeExternal:
		// ok
	case "":
		return ErrValidation("volume_type is required")
	default:
		return ErrValidation("unsupported volume type %q; supported: MANAGED, EXTERNAL", req.VolumeType)
	}
	if req.VolumeType == VolumeTypeExternal && req.StorageLocation == "" {
		return ErrValidation("storage_location is required for EXTERNAL volumes")
	}
	return nil
}
