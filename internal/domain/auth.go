package domain

import "time"

// AuthIdentity links a principal to an external identity provider subject.
type AuthIdentity struct {
	ID            string
	PrincipalID   string
	Provider      string
	Issuer        *string
	Subject       string
	Email         *string
	EmailVerified bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// LocalCredential stores password-based local login credentials.
type LocalCredential struct {
	PrincipalID        string
	Username           string
	PasswordHash       string
	PasswordChangedAt  time.Time
	MustChangePassword bool
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// AuthSession is a server-side interactive login session.
type AuthSession struct {
	ID            string
	PrincipalID   string
	SessionHash   string
	AuthMethod    string
	UserAgent     *string
	IPAddress     *string
	ExpiresAt     time.Time
	IdleExpiresAt time.Time
	LastSeenAt    time.Time
	RevokedAt     *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// AuthRecoveryCode is a single-use recovery credential.
type AuthRecoveryCode struct {
	ID          string
	PrincipalID string
	CodeHash    string
	UsedAt      *time.Time
	ExpiresAt   time.Time
	CreatedAt   time.Time
}

// AuthLoginAttempt records login attempts for abuse protection.
type AuthLoginAttempt struct {
	ID        string
	Username  *string
	IPAddress *string
	Success   bool
	Reason    *string
	CreatedAt time.Time
}

// SetupState tracks whether first-run bootstrap is complete.
type SetupState struct {
	SetupCompleted          bool
	SetupCompletedAt        *time.Time
	SetupCompletedBy        *string
	BootstrapTokenHash      *string
	BootstrapTokenExpiresAt *time.Time
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

// AuthProviderConfig stores runtime-manageable provider settings.
type AuthProviderConfig struct {
	OIDCEnabled         bool
	OIDCIssuerURL       *string
	OIDCJWKSURL         *string
	OIDCAudience        *string
	OIDCClientID        *string
	OIDCClientSecretEnc *string
	OIDCScopes          *string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// WebAuthnCredential stores a passkey bound to a principal.
type WebAuthnCredential struct {
	ID             string
	PrincipalID    string
	CredentialID   string
	PublicKey      string
	SignCount      int64
	Transports     *string
	BackupEligible bool
	BackupState    bool
	CreatedAt      time.Time
	LastUsedAt     *time.Time
}
