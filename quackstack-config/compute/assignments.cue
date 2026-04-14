package duckconfig

platform: compute: assignments: [{
	endpoint:       "local-dev"
	principal:      "__QUACK_DEV_BOOTSTRAP_PRINCIPAL__"
	principal_type: "user"
	is_default:     true
}, {
	endpoint:       "local-dev"
	principal:      "analyst"
	principal_type: "user"
	is_default:     true
}]
