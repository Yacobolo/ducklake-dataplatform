package duckconfig

platform: catalogs: seeded_local: {
	metastore_type: "sqlite"
	dsn:            "__QUACK_DEV_SAMPLE_METASTORE__"
	data_path:      "__QUACK_DEV_SAMPLE_DATA_DIR__"
	comment:        "Local seeded NYC taxi catalog for developer workflows"
}
