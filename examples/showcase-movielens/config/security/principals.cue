package duckconfig

platform: security: principals: {
	ml_admin: {
		name:     ""
		type:     "user"
		is_admin: true
	}
	ml_analyst: {
		name:     ""
		type:     "user"
		is_admin: false
	}
	ml_service: {
		name:     ""
		type:     "service_principal"
		is_admin: false
	}
}
