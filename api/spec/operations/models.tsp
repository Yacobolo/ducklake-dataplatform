using TypeSpec.Http;

namespace Duck;

@route("/models")
@get
@tag("Models")
@summary("List models")
op listModels(@query project_name?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedModels> | StandardErrorResponses;

@route("/models")
@post
@tag("Models")
@summary("Create model")
op createModel(@body body: CreateModelRequest): Created<Model> | MutatingErrorResponses;

@route("/models/{project_name}/{model_name}")
@get
@tag("Models")
@summary("Get model")
op getModel(project_name: string, model_name: string): Ok<Model> | ResourceErrorResponses;

@route("/models/{project_name}/{model_name}")
@patch(#{ implicitOptionality: true })
@tag("Models")
@summary("Update model")
op updateModel(project_name: string, model_name: string, @body body: UpdateModelRequest): Ok<Model> | MutatingErrorResponses;

@route("/models/{project_name}/{model_name}")
@delete
@tag("Models")
@summary("Delete model")
op deleteModel(project_name: string, model_name: string): NoContent | MutatingErrorResponses;

@route("/models/dag")
@get
@tag("Models")
@summary("Get model DAG")
op getModelDAG(@query project_name?: string): Ok<ModelDAG> | StandardErrorResponses;

@route("/model-runs")
@post
@tag("Models")
@summary("Trigger model run")
op triggerModelRun(@body body: TriggerModelRunRequest): Created<ModelRun> | MutatingErrorResponses;

@route("/model-runs")
@get
@tag("Models")
@summary("List model runs")
op listModelRuns(@query status?: string, @query max_results?: int32, @query page_token?: string): Ok<PaginatedModelRuns> | StandardErrorResponses;

@route("/model-runs/{run_id}")
@get
@tag("Models")
@summary("Get model run")
op getModelRun(run_id: string): Ok<ModelRun> | ResourceErrorResponses;

@route("/model-runs/{run_id}/steps")
@get
@tag("Models")
@summary("List model run steps")
op listModelRunSteps(run_id: string): Ok<ModelRunStepList> | ResourceErrorResponses;

@route("/models/{project_name}/{model_name}/tests")
@post
@tag("Models")
@summary("Create model test")
op createModelTest(project_name: string, model_name: string, @body body: CreateModelTestRequest): Created<ModelTest> | MutatingErrorResponses;

@route("/models/{project_name}/{model_name}/tests")
@get
@tag("Models")
@summary("List model tests")
op listModelTests(project_name: string, model_name: string): Ok<ModelTestList> | ResourceErrorResponses;

@route("/models/{project_name}/{model_name}/tests/{test_id}")
@delete
@tag("Models")
@summary("Delete model test")
op deleteModelTest(project_name: string, model_name: string, test_id: string): NoContent | MutatingErrorResponses;

@route("/model-runs/{run_id}/steps/{step_id}/test-results")
@get
@tag("Models")
@summary("List model test results")
op listModelTestResults(run_id: string, step_id: string): Ok<ModelTestResultList> | ResourceErrorResponses;

@route("/models/{project_name}/{model_name}/freshness")
@get
@tag("Models")
@summary("Check model freshness")
op checkModelFreshness(project_name: string, model_name: string): Ok<FreshnessStatus> | ResourceErrorResponses;

@route("/notebook-model-promotions")
@post
@tag("Models")
@summary("Promote notebook to model")
op promoteNotebookToModel(@body body: PromoteNotebookRequest): Created<Model> | MutatingErrorResponses;

@route("/notebook-model-promotions/{notebook_id}")
@delete
@tag("Models")
@summary("Unpublish notebook model")
op unpublishNotebookModel(notebook_id: string): NoContent | ResourceErrorResponses;

@route("/model-runs/{run_id}/cancellations")
@post
@tag("Models")
@summary("Cancel model run")
op cancelModelRun(run_id: string): Ok<ModelRun> | MutatingErrorResponses;

@route("/macros")
@get
@tag("Models")
@summary("List macros")
op listMacros(@query max_results?: int32, @query page_token?: string): Ok<PaginatedMacros> | StandardErrorResponses;

@route("/macros")
@post
@tag("Models")
@summary("Create macro")
op createMacro(@body body: CreateMacroRequest): Created<Macro> | MutatingErrorResponses;

@route("/macros/{macro_name}")
@get
@tag("Models")
@summary("Get macro")
op getMacro(macro_name: string): Ok<Macro> | ResourceErrorResponses;

@route("/macros/{macro_name}")
@patch(#{ implicitOptionality: true })
@tag("Models")
@summary("Update macro")
op updateMacro(macro_name: string, @body body: UpdateMacroRequest): Ok<Macro> | MutatingErrorResponses;

@route("/macros/{macro_name}")
@delete
@tag("Models")
@summary("Delete macro")
op deleteMacro(macro_name: string): NoContent | MutatingErrorResponses;

@route("/macros/{macro_name}/revisions")
@get
@tag("Models")
@summary("List macro revisions")
op listMacroRevisions(macro_name: string): Ok<MacroRevisionList> | ResourceErrorResponses;

@route("/macros/{macro_name}/impacts")
@get
@tag("Models")
@summary("Get macro impact")
op getMacroImpact(macro_name: string, @query max_results?: int32, @query page_token?: string): Ok<MacroImpactList> | ResourceErrorResponses;

@route("/macros/{macro_name}/revision-diffs")
@get
@tag("Models")
@summary("Diff macro revisions")
op diffMacroRevisions(macro_name: string, @query from_version: int32, @query to_version: int32): Ok<MacroRevisionDiff> | ResourceErrorResponses;
