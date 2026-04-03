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

@route("/models/{projectName}/{modelName}")
@get
@tag("Models")
@summary("Get model")
op getModel(projectName: string, modelName: string): Ok<Model> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}")
@patch(#{ implicitOptionality: true })
@tag("Models")
@summary("Update model")
op updateModel(projectName: string, modelName: string, @body body: UpdateModelRequest): Ok<Model> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}")
@delete
@tag("Models")
@summary("Delete model")
op deleteModel(projectName: string, modelName: string): NoContent | MutatingErrorResponses;

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

@route("/model-runs/{runId}")
@get
@tag("Models")
@summary("Get model run")
op getModelRun(runId: string): Ok<ModelRun> | ResourceErrorResponses;

@route("/model-runs/{runId}/steps")
@get
@tag("Models")
@summary("List model run steps")
op listModelRunSteps(runId: string): Ok<ModelRunStepList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@post
@tag("Models")
@summary("Create model test")
op createModelTest(projectName: string, modelName: string, @body body: CreateModelTestRequest): Created<ModelTest> | MutatingErrorResponses;

@route("/models/{projectName}/{modelName}/tests")
@get
@tag("Models")
@summary("List model tests")
op listModelTests(projectName: string, modelName: string): Ok<ModelTestList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/tests/{testId}")
@delete
@tag("Models")
@summary("Delete model test")
op deleteModelTest(projectName: string, modelName: string, testId: string): NoContent | MutatingErrorResponses;

@route("/model-runs/{runId}/steps/{stepId}/test-results")
@get
@tag("Models")
@summary("List model test results")
op listModelTestResults(runId: string, stepId: string): Ok<ModelTestResultList> | ResourceErrorResponses;

@route("/models/{projectName}/{modelName}/freshness")
@get
@tag("Models")
@summary("Check model freshness")
op checkModelFreshness(projectName: string, modelName: string): Ok<FreshnessStatus> | ResourceErrorResponses;

@route("/notebook-model-promotions")
@post
@tag("Models")
@summary("Promote notebook to model")
op promoteNotebookToModel(@body body: PromoteNotebookRequest): Created<Model> | MutatingErrorResponses;

@route("/notebook-model-promotions/{notebookId}")
@delete
@tag("Models")
@summary("Unpublish notebook model")
op unpublishNotebookModel(notebookId: string): NoContent | ResourceErrorResponses;

@route("/model-runs/{runId}/cancellations")
@post
@tag("Models")
@summary("Cancel model run")
op cancelModelRun(runId: string): Ok<ModelRun> | MutatingErrorResponses;

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

@route("/macros/{macroName}")
@get
@tag("Models")
@summary("Get macro")
op getMacro(macroName: string): Ok<Macro> | ResourceErrorResponses;

@route("/macros/{macroName}")
@patch(#{ implicitOptionality: true })
@tag("Models")
@summary("Update macro")
op updateMacro(macroName: string, @body body: UpdateMacroRequest): Ok<Macro> | MutatingErrorResponses;

@route("/macros/{macroName}")
@delete
@tag("Models")
@summary("Delete macro")
op deleteMacro(macroName: string): NoContent | MutatingErrorResponses;

@route("/macros/{macroName}/revisions")
@get
@tag("Models")
@summary("List macro revisions")
op listMacroRevisions(macroName: string): Ok<MacroRevisionList> | ResourceErrorResponses;

@route("/macros/{macroName}/impacts")
@get
@tag("Models")
@summary("Get macro impact")
op getMacroImpact(macroName: string, @query max_results?: int32, @query page_token?: string): Ok<MacroImpactList> | ResourceErrorResponses;

@route("/macros/{macroName}/revision-diffs")
@get
@tag("Models")
@summary("Diff macro revisions")
op diffMacroRevisions(macroName: string, @query from_version: int32, @query to_version: int32): Ok<MacroRevisionDiff> | ResourceErrorResponses;
