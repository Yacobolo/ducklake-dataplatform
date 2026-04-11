package api

// Authored asset schemas.

#assetFields: {
	asset_key:                 #stringProperty
	asset_type:                #refProperty & {#ref: "AssetType"}
	auto_materialize_policy:   #refProperty & {#ref: "AssetAutoMaterializePolicy"}
	created_at:                #createdAtProperty
	created_by:                #stringProperty
	description:               #descriptionProperty
	freshness_policy:          #refProperty & {#ref: "AssetFreshnessPolicy"}
	id:                        #idProperty
	io_profile:                #stringProperty
	is_active:                 #boolProperty
	materialization_policy:    #refProperty & {#ref: "AssetMaterializationPolicy"}
	owner:                     #ownerProperty
	tags:                      #stringArrayProperty
	updated_at:                #updatedAtProperty
	...
}

#assetReferenceFields: {
	asset_id:   #stringProperty
	asset_key:  #stringProperty
	asset_type: #refProperty & {#ref: "AssetType"}
	...
}

#timedStatusFields: {
	created_at: #createdAtProperty
	...
}

#backfillRunFields: #timedStatusFields & {
	asset_id:        #stringProperty
	error_message:   #stringProperty
	finished_at:     #stringProperty
	id:              #idProperty
	partition_from:  #stringProperty
	partition_to:    #stringProperty
	started_at:      #stringProperty
	status:          #statusProperty
	...
}

schemas_assets: {
	Asset: #objectSchema & {
		#fields: #assetFields
	}

	AssetAutoMaterializePolicy: #objectSchema & {
		#fields: {
			downtime_windows_cron_expr: #stringArrayProperty
			min_interval_seconds:       #int64Property
			mode:                       #stringProperty
			on_freshness_breach:        #boolProperty
			on_upstream_materialized:   #boolProperty
			require_all_upstreams:      #boolProperty
			respect_downtime_windows:   #boolProperty
		}
	}

	AssetBackfillDetails: #objectSchema & {
		#fields: {
			request: #refProperty & {#ref: "BackfillRequest"}
			slices:  #arrayRefProperty & {#ref: "BackfillSlice"}
		}
	}

	AssetCheck: #objectSchema & {
		#fields: {
			asset_id:   #stringProperty
			check_type: #stringProperty
			created_at: #createdAtProperty
			enabled:    #enabledProperty
			id:         #idProperty
			name:       #nameProperty
			severity:   #refProperty & {#ref: "AssetCheckSeverity"}
			updated_at: #updatedAtProperty
		}
	}

	AssetCheckInput: #objectSchema & {
		#fields: {
			check_type:  #stringProperty
			config_json: #refProperty & {#ref: "Record"}
			enabled:     #enabledProperty
			name:        #nameProperty
			severity:    #refProperty & {#ref: "AssetCheckSeverity"}
		}
		#required: ["name", "check_type"]
	}

	AssetCheckList: #objectSchema & {
		#fields: {
			data: #arrayRefProperty & {#ref: "AssetCheck"}
		}
		#required: ["data"]
	}

	AssetCheckResult: #objectSchema & {
		#fields: {
			check_id:      #stringProperty
			created_at:    #createdAtProperty
			id:            #idProperty
			message:       #stringProperty
			metrics_json:  #refProperty & {#ref: "Record"}
			partition_key: #stringProperty
			run_id:        #stringProperty
			status:        #statusProperty
		}
	}

	AssetCheckSeverity: #enumSchema & {
		#values: ["ERROR", "WARN"]
	}

	AssetFreshnessBlocker: #objectSchema & {
		#fields: {
			asset:           #refProperty & {#ref: "AssetFreshnessStatus"}
			dependency_type: #stringProperty
		}
	}

	AssetFreshnessBlockersResponse: #objectSchema & {
		#fields: {
			asset:    #refProperty & {#ref: "AssetFreshnessStatus"}
			blockers: #arrayRefProperty & {#ref: "AssetFreshnessBlocker"}
		}
	}

	AssetFreshnessEdge: #objectSchema & {
		#fields: {
			dependency_type: #stringProperty
			from_asset_key:  #stringProperty
			to_asset_key:    #stringProperty
		}
	}

	AssetFreshnessExplanation: #objectSchema & {
		#fields: {
			asset: #refProperty & {#ref: "AssetFreshnessStatus"}
			edges: #arrayRefProperty & {#ref: "AssetFreshnessEdge"}
			nodes: #arrayRefProperty & {#ref: "AssetFreshnessStatus"}
		}
	}

	AssetFreshnessPolicy: #objectSchema & {
		#fields: {
			cron_schedule:   #stringProperty
			max_lag_seconds: #int64Property
		}
	}

	AssetFreshnessReconcileResponse: #objectSchema & {
		#fields: {
			asset:   #refProperty & {#ref: "AssetFreshnessStatus"}
			targets: #arrayRefProperty & {#ref: "AssetFreshnessReconcileTarget"}
		}
	}

	AssetFreshnessReconcileTarget: #objectSchema & {
		#fields: #assetReferenceFields & {
			event_id:         #stringProperty
			freshness_status: #statusProperty
		}
	}

	AssetFreshnessRequirement: #objectSchema & {
		#fields: {
			asset:           #refProperty & {#ref: "AssetFreshnessStatus"}
			dependency_type: #stringProperty
		}
	}

	AssetFreshnessRequirementsResponse: #objectSchema & {
		#fields: {
			asset:        #refProperty & {#ref: "AssetFreshnessStatus"}
			requirements: #arrayRefProperty & {#ref: "AssetFreshnessRequirement"}
		}
	}

	AssetFreshnessStatus: #objectSchema & {
		#fields: #assetReferenceFields & {
			basis:                    #stringArrayProperty
			effective_max_lag_seconds: #int64Property
			freshness_status:         #statusProperty
			last_materialized_at:     #stringProperty
			reason:                   #stringProperty
			stale_since:              #stringProperty
		}
	}

	AssetGraph: #objectSchema & {
		#fields: {
			asset_key:             #stringProperty
			downstream_asset_keys: #stringArrayProperty
			upstream_asset_keys:   #stringArrayProperty
		}
	}

	AssetMaterialization: #objectSchema & {
		#fields: {
			asset_id:        #stringProperty
			created_at:      #createdAtProperty
			id:              #idProperty
			materialized_at: #stringProperty
			partition_key:   #stringProperty
			row_count:       #int64Property
			run_id:          #stringProperty
			schema_hash:     #stringProperty
		}
	}

	AssetMaterializationPolicy: #objectSchema & {
		#fields: {
			allow_concurrent: #boolProperty
			mode:             #stringProperty
		}
	}

	AssetPartition: #objectSchema & {
		#fields: {
			asset_id:      #stringProperty
			created_at:    #createdAtProperty
			id:            #idProperty
			partition_key: #stringProperty
			status:        #statusProperty
			updated_at:    #updatedAtProperty
		}
	}

	AssetRun: #objectSchema & {
		#fields: #timedStatusFields & {
			asset_id:      #stringProperty
			attempt_count: #int32Property
			error_message: #stringProperty
			finished_at:   #stringProperty
			id:            #idProperty
			max_attempts:  #int32Property
			partition_from: #stringProperty
			partition_key:  #stringProperty
			partition_to:   #stringProperty
			run_group_id:   #stringProperty
			started_at:     #stringProperty
			status:         #refProperty & {#ref: "AssetRunStatus"}
			trigger_type:   #refProperty & {#ref: "AssetTriggerType"}
			triggered_by:   #stringProperty
			updated_at:     #updatedAtProperty
		}
	}

	AssetRunStatus: #enumSchema & {
		#values: ["QUEUED", "PLANNING", "RUNNING", "RETRYING", "SUCCESS", "FAILED", "CANCELLED", "SKIPPED", "STALE"]
	}

	AssetTriggerResponse: #objectSchema & {
		#fields: {
			event_id: #stringProperty
			status:   #statusProperty
		}
	}

	AssetTriggerType: #enumSchema & {
		#values: ["MANUAL", "SCHEDULED", "UPSTREAM_UPDATE", "FRESHNESS_BREACH", "API_EVENT", "BACKFILL", "RECONCILER", "PIPELINE"]
	}

	AssetType: #enumSchema & {
		#values: ["TABLE", "VIEW", "MODEL", "NOTEBOOK", "OUTPUT", "DASHBOARD", "SEMANTIC_MODEL", "METRIC", "SEMANTIC_PRE_AGGREGATION", "NOTEBOOK_OUTPUT"]
	}

	BackfillRequest: #objectSchema & {
		#fields: #backfillRunFields & {
			max_parallelism: #int32Property
			requested_by:    #stringProperty
		}
	}

	BackfillSlice: #objectSchema & {
		#fields: #backfillRunFields & {
			attempt_count: #int32Property
			max_attempts:  #int32Property
			partition_key: #stringProperty
			request_id:    #stringProperty
			run_id:        #stringProperty
		}
	}

	CreateAssetBackfillRequest: #objectSchema & {
		#fields: {
			max_parallelism: #int32Property
			partition_from:  #stringProperty
			partition_to:    #stringProperty
		}
		#required: ["partition_from", "partition_to"]
	}

	CreateAssetBackfillResponse: #objectSchema & {
		#fields: {
			request: #refProperty & {#ref: "BackfillRequest"}
			slices:  #arrayRefProperty & {#ref: "BackfillSlice"}
		}
	}

	CreateAssetRequest: #objectSchema & {
		#fields: {
			asset_key:               #stringProperty
			asset_type:              #refProperty & {#ref: "AssetType"}
			auto_materialize_policy: #refProperty & {#ref: "AssetAutoMaterializePolicy"}
			checks:                  #arrayRefProperty & {#ref: "AssetCheckInput"}
			description:             #descriptionProperty
			freshness_policy:        #refProperty & {#ref: "AssetFreshnessPolicy"}
			io_profile:              #stringProperty
			is_active:               #boolProperty
			materialization_policy:  #refProperty & {#ref: "AssetMaterializationPolicy"}
			owner:                   #ownerProperty
			product_slug:            #stringProperty
			tags:                    #stringArrayProperty
			upstream_asset_keys:     #stringArrayProperty
		}
		#required: ["asset_key", "asset_type", "product_slug", "owner"]
	}

	TriggerAssetMaterializationRequest: #objectSchema & {
		#fields: {
			idempotency_key: #stringProperty
			partition_key:   #stringProperty
			payload:         #refProperty & {#ref: "Record"}
		}
	}

	UpdateAssetRequest: #objectSchema & {
		#fields: {
			asset_type:              #refProperty & {#ref: "AssetType"}
			auto_materialize_policy: #refProperty & {#ref: "AssetAutoMaterializePolicy"}
			checks:                  #arrayRefProperty & {#ref: "AssetCheckInput"}
			description:             #descriptionProperty
			freshness_policy:        #refProperty & {#ref: "AssetFreshnessPolicy"}
			io_profile:              #stringProperty
			is_active:               #boolProperty
			materialization_policy:  #refProperty & {#ref: "AssetMaterializationPolicy"}
			owner:                   #ownerProperty
			product_slug:            #stringProperty
			tags:                    #stringArrayProperty
			upstream_asset_keys:     #stringArrayProperty
		}
		#required: ["asset_type", "product_slug", "owner"]
	}
}
