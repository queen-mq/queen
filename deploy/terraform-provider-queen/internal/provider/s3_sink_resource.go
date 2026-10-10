package provider

import (
	"context"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/booldefault"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

var (
	_ resource.Resource                = &s3SinkResource{}
	_ resource.ResourceWithConfigure   = &s3SinkResource{}
	_ resource.ResourceWithImportState = &s3SinkResource{}
)

type s3SinkResource struct {
	c *client
}

func newS3SinkResource() resource.Resource { return &s3SinkResource{} }

type s3SinkModel struct {
	ID               types.String `tfsdk:"id"`
	Cluster          types.String `tfsdk:"cluster"`
	Enabled          types.Bool   `tfsdk:"enabled"`
	Queues           types.String `tfsdk:"queues"`
	Endpoint         types.String `tfsdk:"endpoint"`
	Region           types.String `tfsdk:"region"`
	Bucket           types.String `tfsdk:"bucket"`
	AccessKey        types.String `tfsdk:"access_key"`
	SecretKey        types.String `tfsdk:"secret_key"`
	SecretKeyVersion types.Int64  `tfsdk:"secret_key_version"`
	Prefix           types.String `tfsdk:"prefix"`
	PathStyle        types.Bool   `tfsdk:"path_style"`
	SSE              types.String `tfsdk:"sse"`
	SSEKMSKeyID      types.String `tfsdk:"sse_kms_key_id"`
	Format           types.String `tfsdk:"format"`
	Compression      types.String `tfsdk:"compression"`
	ParquetCodec     types.String `tfsdk:"parquet_codec"`
	Layout           types.String `tfsdk:"layout"`
	Align            types.String `tfsdk:"align"`
	Start            types.String `tfsdk:"start"`
	TargetMB         types.Int64  `tfsdk:"target_mb"`
	MaxWindowMS      types.Int64  `tfsdk:"max_window_ms"`
	Sink             types.String `tfsdk:"sink"`
	SecretKeySet     types.Bool   `tfsdk:"secret_key_set"`
	UpdatedAt        types.String `tfsdk:"updated_at"`
}

func (r *s3SinkResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_s3_sink"
}

func (r *s3SinkResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	text := func(description string) schema.StringAttribute {
		return schema.StringAttribute{Optional: true, MarkdownDescription: description}
	}
	resp.Schema = schema.Schema{
		MarkdownDescription: "The S3 sink of one cluster: the broker mirrors the cluster's queues into a bucket. It is set through the proxy's control plane, so the provider needs `cp_endpoint` and `cp_token`, and the broker has to run with `QUEEN_S3_EMBEDDED=true`. The settings and their defaults are the per-sink ones of the [S3 guide](https://queenmq.com/guides/s3/#reference); one this resource leaves out takes the broker's default.",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "The cluster's slug.",
				PlanModifiers:       []planmodifier.String{stringplanmodifier.UseStateForUnknown()},
			},
			"cluster": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "The slug of the cluster whose queues are mirrored. A cluster has one sink.",
				PlanModifiers:       []planmodifier.String{stringplanmodifier.RequiresReplace()},
			},
			"enabled": schema.BoolAttribute{
				Optional:            true,
				Computed:            true,
				Default:             booldefault.StaticBool(true),
				MarkdownDescription: "False stops the sink and keeps its settings and its secret.",
			},
			"queues": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Comma-separated queue names, or `*` for every queue of the cluster.",
			},
			"endpoint": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "`http://` or `https://`, a host and an optional port, no path.",
			},
			"region": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "The region the requests are signed for.",
			},
			"bucket": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "The bucket's name.",
			},
			"access_key": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "The access key id.",
			},
			"secret_key": schema.StringAttribute{
				Optional:            true,
				Sensitive:           true,
				WriteOnly:           true,
				MarkdownDescription: "The secret access key. It is write-only: it is sent to the broker, which seals it with its encryption key, and it is never stored in the state or the plan. It is sent when the sink is created and whenever `secret_key_version` changes. Needs Terraform 1.11 or OpenTofu 1.11.",
			},
			"secret_key_version": schema.Int64Attribute{
				Optional:            true,
				MarkdownDescription: "Change it to send `secret_key` again, after rotating the key.",
			},
			"prefix":         text("The root of every object key."),
			"path_style":     schema.BoolAttribute{Optional: true, MarkdownDescription: "Put the bucket in the URL path, as MinIO and most self-hosted gateways want."},
			"sse":            text("`AES256` or `aws:kms`, sent with every upload."),
			"sse_kms_key_id": text("The KMS key, with `aws:kms`."),
			"format":         text("`jsonl` or `parquet`."),
			"compression":    text("JSONL only: `zstd`, `gzip` or `none`."),
			"parquet_codec":  text("Parquet only: `zstd` or `snappy`."),
			"layout":         text("`merged`, one object per window, or `per-partition`."),
			"align":          text("`hour`, `day` or `none`: the boundary no window crosses."),
			"start":          text("Where a queue the sink has never read starts: `latest` or `earliest`."),
			"target_mb":      schema.Int64Attribute{Optional: true, MarkdownDescription: "Megabytes of uncompressed records that close a window."},
			"max_window_ms":  schema.Int64Attribute{Optional: true, MarkdownDescription: "The most a window stays open, in milliseconds."},
			"sink":           text("The sink's name in the broker's own records, which a queue's `retentionSinkHold` refers to."),
			"secret_key_set": schema.BoolAttribute{
				Computed:            true,
				MarkdownDescription: "Whether the broker holds a secret for this sink.",
			},
			"updated_at": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "When the broker last stored the sink.",
			},
		},
	}
}

func (r *s3SinkResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	r.c = clientOf(req.ProviderData, resp)
}

// The broker's field name of each setting.
func (m *s3SinkModel) config() map[string]any {
	c := map[string]any{}
	put := func(field string, v types.String) {
		if !v.IsNull() && !v.IsUnknown() {
			c[field] = v.ValueString()
		}
	}
	put("queues", m.Queues)
	put("endpoint", m.Endpoint)
	put("region", m.Region)
	put("bucket", m.Bucket)
	put("accessKey", m.AccessKey)
	put("prefix", m.Prefix)
	put("sse", m.SSE)
	put("sseKmsKeyId", m.SSEKMSKeyID)
	put("format", m.Format)
	put("compression", m.Compression)
	put("parquetCodec", m.ParquetCodec)
	put("layout", m.Layout)
	put("align", m.Align)
	put("start", m.Start)
	put("sink", m.Sink)
	if !m.PathStyle.IsNull() && !m.PathStyle.IsUnknown() {
		c["pathStyle"] = m.PathStyle.ValueBool()
	}
	if !m.TargetMB.IsNull() && !m.TargetMB.IsUnknown() {
		c["targetMb"] = m.TargetMB.ValueInt64()
	}
	if !m.MaxWindowMS.IsNull() && !m.MaxWindowMS.IsUnknown() {
		c["maxWindowMs"] = m.MaxWindowMS.ValueInt64()
	}
	return c
}

// take copies the stored sink into the model. The broker stores exactly the
// fields it was sent, so one it does not hold is null here too.
func (m *s3SinkModel) take(s *s3Sink) {
	text := func(field string) types.String {
		switch v := s.Config[field].(type) {
		case string:
			return types.StringValue(v)
		case nil:
			return types.StringNull()
		default:
			return types.StringValue(fmt.Sprint(v))
		}
	}
	number := func(field string) types.Int64 {
		if v, ok := s.Config[field].(float64); ok {
			return types.Int64Value(int64(v))
		}
		return types.Int64Null()
	}
	m.ID = types.StringValue(s.Cluster)
	m.Cluster = types.StringValue(s.Cluster)
	m.Enabled = types.BoolValue(s.Enabled)
	m.Queues = text("queues")
	m.Endpoint = text("endpoint")
	m.Region = text("region")
	m.Bucket = text("bucket")
	m.AccessKey = text("accessKey")
	m.Prefix = text("prefix")
	m.SSE = text("sse")
	m.SSEKMSKeyID = text("sseKmsKeyId")
	m.Format = text("format")
	m.Compression = text("compression")
	m.ParquetCodec = text("parquetCodec")
	m.Layout = text("layout")
	m.Align = text("align")
	m.Start = text("start")
	m.Sink = text("sink")
	if v, ok := s.Config["pathStyle"].(bool); ok {
		m.PathStyle = types.BoolValue(v)
	} else {
		m.PathStyle = types.BoolNull()
	}
	m.TargetMB = number("targetMb")
	m.MaxWindowMS = number("maxWindowMs")
	m.SecretKeySet = types.BoolValue(s.SecretKeySet)
	m.UpdatedAt = types.StringValue(s.UpdatedAt)
	// Write-only: never in the state.
	m.SecretKey = types.StringNull()
}

// The write-only secret is read from the configuration in Create and Update:
// it is the only place it exists.

func (r *s3SinkResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var plan s3SinkModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	var secret types.String
	resp.Diagnostics.Append(req.Config.GetAttribute(ctx, path.Root("secret_key"), &secret)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if secret.IsNull() || secret.ValueString() == "" {
		resp.Diagnostics.AddAttributeError(path.Root("secret_key"), "No secret key",
			"A new sink needs secret_key: the broker has none to keep.")
		return
	}
	version := plan.SecretKeyVersion
	s, err := r.c.putS3Sink(ctx, plan.Cluster.ValueString(), plan.Enabled.ValueBool(), plan.config(), secret.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("The S3 sink could not be set", err.Error())
		return
	}
	plan.take(s)
	plan.SecretKeyVersion = version
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *s3SinkResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var state s3SinkModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	s, err := r.c.getS3Sink(ctx, state.ID.ValueString())
	if e, ok := err.(*apiError); ok && e.Status == 404 && (e.Code == "s3_unset" || e.Code == "cluster_unknown") {
		resp.State.RemoveResource(ctx)
		return
	}
	if err != nil {
		resp.Diagnostics.AddError("The S3 sink could not be read", err.Error())
		return
	}
	version := state.SecretKeyVersion
	state.take(s)
	state.SecretKeyVersion = version
	resp.Diagnostics.Append(resp.State.Set(ctx, &state)...)
}

func (r *s3SinkResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan, state s3SinkModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	var secret types.String
	resp.Diagnostics.Append(req.Config.GetAttribute(ctx, path.Root("secret_key"), &secret)...)
	if resp.Diagnostics.HasError() {
		return
	}
	// The secret goes again only when its version moved: left out, the
	// broker keeps the one it has.
	send := ""
	if !plan.SecretKeyVersion.Equal(state.SecretKeyVersion) {
		if secret.IsNull() || secret.ValueString() == "" {
			resp.Diagnostics.AddAttributeError(path.Root("secret_key"), "No secret key",
				"secret_key_version changed, which sends the secret again, but secret_key is not set.")
			return
		}
		send = secret.ValueString()
	}
	version := plan.SecretKeyVersion
	s, err := r.c.putS3Sink(ctx, plan.Cluster.ValueString(), plan.Enabled.ValueBool(), plan.config(), send)
	if err != nil {
		resp.Diagnostics.AddError("The S3 sink could not be set", err.Error())
		return
	}
	plan.take(s)
	plan.SecretKeyVersion = version
	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *s3SinkResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var state s3SinkModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := r.c.deleteS3Sink(ctx, state.ID.ValueString()); err != nil && !notFound(err) {
		resp.Diagnostics.AddError("The S3 sink could not be removed", err.Error())
	}
}

func (r *s3SinkResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("id"), req, resp)
}
