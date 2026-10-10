// Package provider is the Terraform provider for Queen MQ.
package provider

import (
	"context"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

type queenProvider struct {
	version string
}

func New(version string) func() provider.Provider {
	return func() provider.Provider { return &queenProvider{version: version} }
}

type providerModel struct {
	Endpoint   types.String `tfsdk:"endpoint"`
	Token      types.String `tfsdk:"token"`
	CPEndpoint types.String `tfsdk:"cp_endpoint"`
	CPToken    types.String `tfsdk:"cp_token"`
}

func (p *queenProvider) Metadata(_ context.Context, _ provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "queen"
	resp.Version = p.version
}

func (p *queenProvider) Schema(_ context.Context, _ provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Declares what lives inside a [Queen MQ](https://queenmq.com) broker: the configuration of its queues, and a cluster's S3 sink.",
		Attributes: map[string]schema.Attribute{
			"endpoint": schema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "The broker's address, or the proxy's in front of it: `http://queen.queen.svc:6632`. Also read from `QUEEN_ENDPOINT`.",
			},
			"token": schema.StringAttribute{
				Optional:            true,
				Sensitive:           true,
				MarkdownDescription: "Sent as `Authorization: Bearer`: a proxy API key (`qk_...`) or a JWT. Leave it out for a broker port with no authentication. Also read from `QUEEN_TOKEN`.",
			},
			"cp_endpoint": schema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "The proxy's address, for the resources set through its control plane (`queen_s3_sink`): `http://queen-proxy.queen.svc:6711`. Also read from `QUEEN_CP_ENDPOINT`.",
			},
			"cp_token": schema.StringAttribute{
				Optional:            true,
				Sensitive:           true,
				MarkdownDescription: "The control-plane token (`QUEEN_PROXY_CP_TOKEN` on the broker). Also read from `QUEEN_CP_TOKEN`.",
			},
		},
	}
}

func (p *queenProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var m providerModel
	resp.Diagnostics.Append(req.Config.Get(ctx, &m)...)
	if resp.Diagnostics.HasError() {
		return
	}
	// A value that is only known after another resource is applied cannot
	// configure the provider for this plan.
	for name, v := range map[string]types.String{"endpoint": m.Endpoint, "token": m.Token, "cp_endpoint": m.CPEndpoint, "cp_token": m.CPToken} {
		if v.IsUnknown() {
			resp.Diagnostics.AddAttributeError(path.Root(name), "Unknown value",
				"The provider cannot be configured with a value that is not known yet. Set it directly, or through its environment variable.")
		}
	}
	if resp.Diagnostics.HasError() {
		return
	}
	pick := func(v types.String, env string) string {
		if !v.IsNull() {
			return v.ValueString()
		}
		return os.Getenv(env)
	}
	endpoint := pick(m.Endpoint, "QUEEN_ENDPOINT")
	if endpoint == "" {
		resp.Diagnostics.AddAttributeError(path.Root("endpoint"), "No broker address",
			"Set endpoint, or QUEEN_ENDPOINT, to the broker's address: http://host:6632.")
		return
	}
	c := newClient(endpoint, pick(m.Token, "QUEEN_TOKEN"), pick(m.CPEndpoint, "QUEEN_CP_ENDPOINT"), pick(m.CPToken, "QUEEN_CP_TOKEN"))
	resp.ResourceData = c
	resp.DataSourceData = c
}

func (p *queenProvider) Resources(_ context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		newQueueResource,
		newS3SinkResource,
	}
}

func (p *queenProvider) DataSources(_ context.Context) []func() datasource.DataSource {
	return nil
}

// clientOf is the provider's client as a resource receives it.
func clientOf(data any, resp *resource.ConfigureResponse) *client {
	if data == nil {
		return nil
	}
	c, ok := data.(*client)
	if !ok {
		resp.Diagnostics.AddError("Unexpected provider data", "The provider handed this resource something that is not its client. This is a bug in the provider.")
		return nil
	}
	return c
}
