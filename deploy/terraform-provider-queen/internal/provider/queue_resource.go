package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/boolplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/int64planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &queueResource{}
	_ resource.ResourceWithConfigure   = &queueResource{}
	_ resource.ResourceWithImportState = &queueResource{}
)

type queueResource struct {
	c *client
}

func newQueueResource() resource.Resource { return &queueResource{} }

type queueModel struct {
	ID                        types.String `tfsdk:"id"`
	Name                      types.String `tfsdk:"name"`
	Namespace                 types.String `tfsdk:"namespace"`
	Task                      types.String `tfsdk:"task"`
	LeaseTime                 types.Int64  `tfsdk:"lease_time"`
	RetryLimit                types.Int64  `tfsdk:"retry_limit"`
	DeadLetterQueue           types.Bool   `tfsdk:"dead_letter_queue"`
	DlqAfterMaxRetries        types.Bool   `tfsdk:"dlq_after_max_retries"`
	DedupWindowSeconds        types.Int64  `tfsdk:"dedup_window_seconds"`
	DelayedProcessing         types.Int64  `tfsdk:"delayed_processing"`
	WindowBuffer              types.Int64  `tfsdk:"window_buffer"`
	RetentionEnabled          types.Bool   `tfsdk:"retention_enabled"`
	RetentionSeconds          types.Int64  `tfsdk:"retention_seconds"`
	CompletedRetentionSeconds types.Int64  `tfsdk:"completed_retention_seconds"`
	MaxWaitTimeSeconds        types.Int64  `tfsdk:"max_wait_time_seconds"`
	EncryptionEnabled         types.Bool   `tfsdk:"encryption_enabled"`
	DeleteOnDestroy           types.Bool   `tfsdk:"delete_on_destroy"`
}

func (r *queueResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_queue"
}

// An option the configuration leaves out is the broker's: unknown until the
// first apply, and kept as it is afterwards.
func seconds(description string) schema.Int64Attribute {
	return schema.Int64Attribute{
		Optional:            true,
		Computed:            true,
		MarkdownDescription: description,
		Validators:          []validator.Int64{notNegative{}},
		PlanModifiers:       []planmodifier.Int64{int64planmodifier.UseStateForUnknown()},
	}
}

func flag(description string) schema.BoolAttribute {
	return schema.BoolAttribute{
		Optional:            true,
		Computed:            true,
		MarkdownDescription: description,
		PlanModifiers:       []planmodifier.Bool{boolplanmodifier.UseStateForUnknown()},
	}
}

func label(description string) schema.StringAttribute {
	return schema.StringAttribute{
		Optional:            true,
		Computed:            true,
		MarkdownDescription: description,
		PlanModifiers:       []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
	}
}

func (r *queueResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "The configuration of one queue. A queue is created by the first push that names it; this resource creates it ahead of that, or takes over one that exists, and keeps its options as written here. An option you leave out stays as the broker has it.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "The queue's name.",
				PlanModifiers:       []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "The queue's name. Changing it makes another queue.",
				PlanModifiers:       []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"namespace": label("A label a discovery pop matches on."),
			"task":      label("A label a discovery pop matches on."),
			"lease_time": seconds(
				"Seconds a pop's lease lasts unless the pop asks for another."),
			"retry_limit": seconds(
				"How many `failed` acks redeliver a message before it goes to the dead-letter queue."),
			"dead_letter_queue": flag(
				"File a message that ran out of retries in the dead-letter queue."),
			"dlq_after_max_retries": flag(
				"With `dead_letter_queue`: both off, a message that ran out of retries is dropped."),
			"dedup_window_seconds": seconds(
				"Seconds a partition remembers `transactionId`s. 0 turns dedup off."),
			"delayed_processing": seconds(
				"Seconds after its push before a message can be delivered."),
			"window_buffer": seconds(
				"A partition delivers nothing until it has been quiet this many seconds."),
			"retention_enabled": flag(
				"Must be true for `retention_seconds` and `completed_retention_seconds` to act."),
			"retention_seconds": seconds(
				"Remove messages older than this, consumed or not."),
			"completed_retention_seconds": seconds(
				"Remove messages older than this that every group's cursor has passed."),
			"max_wait_time_seconds": seconds(
				"Remove messages older than this, consumed or not, whatever `retention_enabled` says."),
			"encryption_enabled": flag(
				"Encrypt payloads at rest. The broker needs `QUEEN_ENCRYPTION_KEY`."),
			"delete_on_destroy": schema.BoolAttribute{
				Optional:            true,
				Computed:            true,
				Default:             booldefault.StaticBool(false),
				MarkdownDescription: "Destroying this resource deletes the queue **and every message in it** only when this is true. By default the queue is left as it is and only forgotten.",
			},
		},
	}
}

func (r *queueResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = clientOf(req.ProviderData, resp)
}

func int64Of(v types.Int64) *int64 {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	x := v.ValueInt64()
	return &x
}

func boolOf(v types.Bool) *bool {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	x := v.ValueBool()
	return &x
}

func stringOf(v types.String) *string {
	if v.IsNull() || v.IsUnknown() {
		return nil
	}
	x := v.ValueString()
	return &x
}

func int64Value(v *int64) types.Int64 {
	if v == nil {
		return types.Int64Null()
	}
	return types.Int64Value(*v)
}

func boolValue(v *bool) types.Bool {
	if v == nil {
		return types.BoolNull()
	}
	return types.BoolValue(*v)
}

func (m *queueModel) options() queueOptions {
	return queueOptions{
		LeaseTime:                 int64Of(m.LeaseTime),
		RetryLimit:                int64Of(m.RetryLimit),
		DeadLetterQueue:           boolOf(m.DeadLetterQueue),
		DlqAfterMaxRetries:        boolOf(m.DlqAfterMaxRetries),
		DedupWindowSeconds:        int64Of(m.DedupWindowSeconds),
		DelayedProcessing:         int64Of(m.DelayedProcessing),
		WindowBuffer:              int64Of(m.WindowBuffer),
		RetentionEnabled:          boolOf(m.RetentionEnabled),
		RetentionSeconds:          int64Of(m.RetentionSeconds),
		CompletedRetentionSeconds: int64Of(m.CompletedRetentionSeconds),
		MaxWaitTimeSeconds:        int64Of(m.MaxWaitTimeSeconds),
		EncryptionEnabled:         boolOf(m.EncryptionEnabled),
	}
}

// take copies what the broker stores into the model.
func (m *queueModel) take(q *queue) {
	m.ID = types.StringValue(q.Name)
	m.Name = types.StringValue(q.Name)
	m.Namespace = types.StringValue(q.Namespace)
	m.Task = types.StringValue(q.Task)
	o := q.Options
	m.LeaseTime = int64Value(o.LeaseTime)
	m.RetryLimit = int64Value(o.RetryLimit)
	m.DeadLetterQueue = boolValue(o.DeadLetterQueue)
	m.DlqAfterMaxRetries = boolValue(o.DlqAfterMaxRetries)
	m.DedupWindowSeconds = int64Value(o.DedupWindowSeconds)
	m.DelayedProcessing = int64Value(o.DelayedProcessing)
	m.WindowBuffer = int64Value(o.WindowBuffer)
	m.RetentionEnabled = boolValue(o.RetentionEnabled)
	m.RetentionSeconds = int64Value(o.RetentionSeconds)
	m.CompletedRetentionSeconds = int64Value(o.CompletedRetentionSeconds)
	m.MaxWaitTimeSeconds = int64Value(o.MaxWaitTimeSeconds)
	m.EncryptionEnabled = boolValue(o.EncryptionEnabled)
}

// apply sends the plan and reads the queue back.
func (r *queueResource) apply(ctx context.Context, plan *queueModel) error {
	name := plan.Name.ValueString()
	if err := r.c.configureQueue(ctx, name, stringOf(plan.Namespace), stringOf(plan.Task), plan.options()); err != nil {
		return err
	}
	q, err := r.c.getQueue(ctx, name)
	if err != nil {
		return err
	}
	plan.take(q)
	return nil
}

func (r *queueResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan queueModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := r.apply(ctx, &plan); err != nil {
		resp.Diagnostics.AddError("The queue could not be configured", err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *queueResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state queueModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	q, err := r.c.getQueue(ctx, state.ID.ValueString())
	if notFound(err) {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("The queue could not be read", err.Error())
		return
	}
	state.take(q)
	if state.DeleteOnDestroy.IsNull() {
		state.DeleteOnDestroy = types.BoolValue(false)
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *queueResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan queueModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := r.apply(ctx, &plan); err != nil {
		resp.Diagnostics.AddError("The queue could not be configured", err.Error())
		return
	}
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *queueResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state queueModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	name := state.ID.ValueString()
	if !state.DeleteOnDestroy.ValueBool() {
		resp.Diagnostics.AddWarning("The queue was left in place",
			fmt.Sprintf("Queue %q and its messages still exist on the broker: delete_on_destroy is false, so it was only removed from the state.", name))
		return
	}
	if err := r.c.deleteQueue(ctx, name); err != nil && !notFound(err) {
		resp.Diagnostics.AddError("The queue could not be deleted", err.Error())
	}
}

func (r *queueResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}

// notNegative refuses a negative number of seconds or retries, which the
// broker would store as it is.
type notNegative struct{}

func (notNegative) Description(context.Context) string         { return "must not be negative" }
func (notNegative) MarkdownDescription(context.Context) string { return "must not be negative" }
func (notNegative) ValidateInt64(_ context.Context, req validator.Int64Request, resp *validator.Int64Response) {
	if req.ConfigValue.IsNull() || req.ConfigValue.IsUnknown() {
		return
	}
	if req.ConfigValue.ValueInt64() < 0 {
		resp.Diagnostics.AddAttributeError(req.Path, "Negative value",
			fmt.Sprintf("%d: this must not be negative.", req.ConfigValue.ValueInt64()))
	}
}
