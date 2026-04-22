package api

// Authored asset schemas.

#assetFields: {
	id:                        #idProperty
	asset_key:                 #stringProperty
	asset_type:                #refProperty & {#ref: "AssetType"}
	owner:                     #ownerProperty
	description:               #descriptionProperty
	tags:                      #stringArrayProperty
	freshness_policy:          #refProperty & {#ref: "AssetFreshnessPolicy"}
	materialization_policy:    #refProperty & {#ref: "AssetMaterializationPolicy"}
	auto_materialize_policy:   #refProperty & {#ref: "AssetAutoMaterializePolicy"}
	io_profile:                #stringProperty
	is_active:                 #boolProperty
	created_by:                #stringProperty
	created_at:                #createdAtProperty
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
	finished_at:     #dateTimeProperty
	id:              #idProperty
	partition_from:  #stringProperty
	partition_to:    #stringProperty
	started_at:      #dateTimeProperty
	status:          #statusProperty
	...
}

schemas_assets: {
	Asset: #objectSchema & {
		example: {
			id:          "asset_01hzyorders"
			asset_key:   "mart.orders_daily"
			asset_type:  "TABLE"
			owner:       "team-analytics"
			description: "Daily order fact table powering revenue and fulfillment reporting."
			tags:        ["finance", "orders", "gold"]
			freshness_policy: {
				max_lag_seconds: 21600
				cron_schedule:   "0 */6 * * *"
			}
			materialization_policy: {
				mode:             "incremental"
				allow_concurrent: false
			}
			auto_materialize_policy: {
				mode:                     "scheduled"
				min_interval_seconds:     21600
				require_all_upstreams:    true
				on_freshness_breach:      true
				on_upstream_materialized: true
				respect_downtime_windows: true
				downtime_windows_cron_expr: ["0 0 * * 0"]
			}
			io_profile:  "warehouse-heavy"
			is_active:   true
			created_by:  "alice@example.com"
			created_at:  "2026-04-01T06:00:00Z"
			updated_at:  "2026-04-13T08:15:00Z"
		}
		#fields: #assetFields
	}

	AssetAutoMaterializePolicy: #objectSchema & {
		#fields: {
			mode:                       #stringProperty
			min_interval_seconds:       #int64Property
			require_all_upstreams:      #boolProperty
			on_freshness_breach:        #boolProperty
			on_upstream_materialized:   #boolProperty
			respect_downtime_windows:   #boolProperty
			downtime_windows_cron_expr: #stringArrayProperty
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
			id:         #idProperty
			asset_id:   #stringProperty
			name:       #nameProperty
			check_type: #stringProperty
			severity:   #refProperty & {#ref: "AssetCheckSeverity"}
			enabled:    #enabledProperty
			created_at: #createdAtProperty
			updated_at: #updatedAtProperty
		}
	}

	AssetCheckInput: #objectSchema & {
		example: {
			name:       "row_count_non_zero"
			check_type: "row_count_threshold"
			severity:   "ERROR"
			enabled:    true
			config_json: {
				operator:    ">"
				threshold:   0
				aggregation: "row_count"
			}
		}
		#fields: {
			name:        #nameProperty
			check_type:  #stringProperty
			severity:    #refProperty & {#ref: "AssetCheckSeverity"}
			enabled:     #enabledProperty
			config_json: #anyMapProperty
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
		example: {
			id:            "checkres_01hzyasset"
			check_id:      "check_01hzyrowcount"
			run_id:        "run_01hzydailyorders"
			partition_key: "2026-04-13"
			status:        "PASS"
			message:       "Row count exceeded the configured minimum threshold."
			metrics_json: {
				row_count: 152340
				threshold: 0
			}
			created_at: "2026-04-13T08:04:00Z"
		}
		#fields: {
			id:            #idProperty
			check_id:      #stringProperty
			run_id:        #stringProperty
			partition_key: #stringProperty
			status:        #statusProperty
			message:       #stringProperty
			metrics_json:  #anyMapProperty
			created_at:    #createdAtProperty
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
			from_asset_key:  #stringProperty
			to_asset_key:    #stringProperty
			dependency_type: #stringProperty
		}
	}

	AssetFreshnessExplanation: #objectSchema & {
		#fields: {
			asset: #refProperty & {#ref: "AssetFreshnessStatus"}
			nodes: #arrayRefProperty & {#ref: "AssetFreshnessStatus"}
			edges: #arrayRefProperty & {#ref: "AssetFreshnessEdge"}
		}
	}

	AssetFreshnessPolicy: #objectSchema & {
		#fields: {
			max_lag_seconds: #int64Property
			cron_schedule:   #stringProperty
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
			freshness_status: #statusProperty
			event_id:         #stringProperty
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
		example: {
			asset_id:                   "asset_01hzyorders"
			asset_key:                  "mart.orders_daily"
			asset_type:                 "TABLE"
			freshness_status:           "HEALTHY"
			effective_max_lag_seconds:  21600
			last_materialized_at:       "2026-04-13T06:05:00Z"
			stale_since:                ""
			reason:                     "Latest successful materialization is within the freshness target."
			basis:                      ["asset policy", "upstream materialization"]
		}
		#fields: #assetReferenceFields & {
			freshness_status:         #statusProperty
			effective_max_lag_seconds: #int64Property
			last_materialized_at:     #dateTimeProperty
			stale_since:              #dateTimeProperty
			reason:                   #stringProperty
			basis:                    #stringArrayProperty
		}
	}

	AssetGraph: #objectSchema & {
		example: {
			asset_key:             "mart.orders_daily"
			upstream_asset_keys:   ["stg.orders", "dim.customers"]
			downstream_asset_keys: ["prod.customer_360", "dash.revenue_overview"]
		}
		#fields: {
			asset_key:             #stringProperty
			upstream_asset_keys:   #stringArrayProperty
			downstream_asset_keys: #stringArrayProperty
		}
	}

	AssetMaterialization: #objectSchema & {
		#fields: {
			id:              #idProperty
			asset_id:        #stringProperty
			run_id:          #stringProperty
			partition_key:   #stringProperty
			row_count:       #int64Property
			schema_hash:     #stringProperty
			materialized_at: #dateTimeProperty
			created_at:      #createdAtProperty
		}
	}

	AssetMaterializationPolicy: #objectSchema & {
		#fields: {
			mode:             #stringProperty
			allow_concurrent: #boolProperty
		}
	}

	AssetPartition: #objectSchema & {
		#fields: {
			id:            #idProperty
			asset_id:      #stringProperty
			partition_key: #stringProperty
			status:        #statusProperty
			created_at:    #createdAtProperty
			updated_at:    #updatedAtProperty
		}
	}

	AssetRun: #objectSchema & {
		example: {
			id:             "run_01hzydailyorders"
			asset_id:       "asset_01hzyorders"
			run_group_id:   "rungroup_01hzyscheduled"
			partition_key:  "2026-04-13"
			partition_from: "2026-04-13"
			partition_to:   "2026-04-13"
			status:         "SUCCESS"
			trigger_type:   "SCHEDULED"
			triggered_by:   "system:scheduler"
			attempt_count:  1
			max_attempts:   3
			started_at:     "2026-04-13T06:00:00Z"
			finished_at:    "2026-04-13T06:05:00Z"
			error_message:  ""
			created_at:     "2026-04-13T06:00:00Z"
			updated_at:     "2026-04-13T06:05:00Z"
		}
		#fields: #timedStatusFields & {
			id:            #idProperty
			asset_id:      #stringProperty
			run_group_id:   #stringProperty
			partition_key:  #stringProperty
			partition_from: #stringProperty
			partition_to:   #stringProperty
			status:         #refProperty & {#ref: "AssetRunStatus"}
			trigger_type:   #refProperty & {#ref: "AssetTriggerType"}
			triggered_by:   #stringProperty
			attempt_count: #int32Property
			max_attempts:  #int32Property
			started_at:     #dateTimeProperty
			finished_at:    #dateTimeProperty
			error_message:  #stringProperty
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
			requested_by:    #stringProperty
			max_parallelism: #int32Property
		}
	}

	BackfillSlice: #objectSchema & {
		#fields: #backfillRunFields & {
			request_id:    #stringProperty
			partition_key: #stringProperty
			run_id:        #stringProperty
			attempt_count: #int32Property
			max_attempts:  #int32Property
		}
	}

	CreateAssetBackfillRequest: #objectSchema & {
		example: {
			partition_from:  "2026-04-01"
			partition_to:    "2026-04-07"
			max_parallelism: 4
		}
		#fields: {
			partition_from:  #stringProperty
			partition_to:    #stringProperty
			max_parallelism: #int32Property
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
		example: {
			asset_key:    "mart.orders_daily"
			asset_type:   "TABLE"
			owner:        "team-analytics"
			description:  "Daily order fact table powering revenue and fulfillment reporting."
			tags:         ["finance", "orders", "gold"]
			freshness_policy: {
				max_lag_seconds: 21600
				cron_schedule:   "0 */6 * * *"
			}
			materialization_policy: {
				mode:             "incremental"
				allow_concurrent: false
			}
			auto_materialize_policy: {
				mode:                 "scheduled"
				min_interval_seconds: 21600
			}
			io_profile:          "warehouse-heavy"
			is_active:           true
			upstream_asset_keys: ["stg.orders", "dim.customers"]
			checks: [
				{
					name:       "row_count_non_zero"
					check_type: "row_count_threshold"
					severity:   "ERROR"
					enabled:    true
					config_json: {
						operator:  ">"
						threshold: 0
					}
				},
			]
		}
		#fields: {
			asset_key:               #stringProperty
			asset_type:              #refProperty & {#ref: "AssetType"}
			owner:                   #ownerProperty
			description:             #descriptionProperty
			tags:                    #stringArrayProperty
			freshness_policy:        #refProperty & {#ref: "AssetFreshnessPolicy"}
			materialization_policy:  #refProperty & {#ref: "AssetMaterializationPolicy"}
			auto_materialize_policy: #refProperty & {#ref: "AssetAutoMaterializePolicy"}
			io_profile:              #stringProperty
			is_active:               #boolProperty
			upstream_asset_keys:     #stringArrayProperty
			checks:                  #arrayRefProperty & {#ref: "AssetCheckInput"}
		}
		#required: ["asset_key", "asset_type", "owner"]
	}

	TriggerAssetMaterializationRequest: #objectSchema & {
		#fields: {
			partition_key:   #stringProperty
			idempotency_key: #stringProperty
			payload:         #anyMapProperty
		}
	}

	UpdateAssetRequest: #objectSchema & {
		#fields: {
			asset_type:              #refProperty & {#ref: "AssetType"}
			owner:                   #ownerProperty
			description:             #descriptionProperty
			tags:                    #stringArrayProperty
			freshness_policy:        #refProperty & {#ref: "AssetFreshnessPolicy"}
			materialization_policy:  #refProperty & {#ref: "AssetMaterializationPolicy"}
			auto_materialize_policy: #refProperty & {#ref: "AssetAutoMaterializePolicy"}
			io_profile:              #stringProperty
			is_active:               #boolProperty
			upstream_asset_keys:     #stringArrayProperty
			checks:                  #arrayRefProperty & {#ref: "AssetCheckInput"}
		}
	}
}
