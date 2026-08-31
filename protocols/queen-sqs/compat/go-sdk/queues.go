package main

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// The queue attributes this suite names by hand. `queen.partitions` is this
// facade's own addition to SQS's vocabulary (PLAN_QUEEN_SQS.md: a standard
// queue is M synthesized partitions), and it is asserted rather than ignored
// because the rig runs at 8 where the shipped default is 64 — a suite that
// silently read 64 here would be reading a facade that ignored its own config.
const (
	attrPartitions    = "queen.partitions"
	attrVisibility    = "VisibilityTimeout"
	attrRetention     = "MessageRetentionPeriod"
	attrMaxSize       = "MaximumMessageSize"
	attrDelay         = "DelaySeconds"
	attrWaitTime      = "ReceiveMessageWaitTimeSeconds"
	attrQueueArn      = "QueueArn"
	attrRedrivePolicy = "RedrivePolicy"
	attrFifo          = "FifoQueue"
	attrMessages      = "ApproximateNumberOfMessages"
	attrNotVisible    = "ApproximateNumberOfMessagesNotVisible"
	attrDelayed       = "ApproximateNumberOfMessagesDelayed"
)

func tQueueCrud(ctx context.Context, r *rig) {
	attributes := map[string]string{
		attrVisibility: "30",
		attrRetention:  "3600",
		attrMaxSize:    "262144",
		attrDelay:      "0",
		attrWaitTime:   "0",
	}
	tags := map[string]string{"team": "billing", "env": "rig"}
	queue, made := r.newQueue(ctx, "CreateQueue", "crud", attributes, tags)
	if !made {
		return
	}
	checkEq("CreateQueue.url", queue.url, r.urlOfQueue(queue.name))

	again, err := r.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: &queue.name, Attributes: attributes, Tags: tags,
	})
	if checkNoErr("CreateQueue.idempotent_identical_request_succeeds", err) {
		checkEq("CreateQueue.idempotent_identical_request", aws.ToString(again.QueueUrl), queue.url)
	}

	// AWS answers QueueNameExists only when the request's OWN attributes
	// disagree with the stored ones. This one does.
	_, err = r.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: &queue.name, Attributes: map[string]string{attrVisibility: "45"},
	})
	expectAPIError("CreateQueue.conflicting_attribute_refused", err,
		"QueueNameExists", new(*types.QueueNameExists))

	// ...and a repeat that names NO attributes has nothing to disagree with.
	// This is the idempotent create every framework performs at worker startup
	// against a queue Terraform made with non-default attributes.
	bare, err := r.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queue.name})
	if checkNoErr("CreateQueue.repeat_without_attributes_succeeds", err) {
		checkEq("CreateQueue.repeat_without_attributes_is_idempotent", aws.ToString(bare.QueueUrl), queue.url)
	}

	// The same rule one step in: a SUBSET agrees about what it names and says
	// nothing about the rest, so the omitted MessageRetentionPeriod is not a
	// disagreement.
	subset, err := r.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: &queue.name, Attributes: map[string]string{attrVisibility: "30"},
	})
	if checkNoErr("CreateQueue.repeat_with_a_subset_succeeds", err) {
		checkEq("CreateQueue.repeat_with_a_subset_is_idempotent", aws.ToString(subset.QueueUrl), queue.url)
	}

	url, err := r.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queue.name})
	if checkNoErr("GetQueueUrl.succeeds", err) {
		checkEq("GetQueueUrl.round_trip", aws.ToString(url.QueueUrl), queue.url)
	}

	listed, err := r.sqs.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: &queue.name})
	if checkNoErr("ListQueues.succeeds", err) {
		checkEq("ListQueues.prefix_filters", listed.QueueUrls, []string{queue.url})
	}

	absentPrefix := "no-such-prefix-" + r.run
	empty, err := r.sqs.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: &absentPrefix})
	if checkNoErr("ListQueues.prefix_that_matches_nothing_succeeds", err) {
		check("ListQueues.prefix_that_matches_nothing", len(empty.QueueUrls) == 0,
			"got "+joinURLs(empty.QueueUrls))
	}
}

func tQueueAttributes(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "GetQueueAttributes", "attrs", map[string]string{
		attrVisibility: "30",
		attrRetention:  "3600",
	}, nil)
	if !made {
		return
	}

	all := r.attributes(ctx, "GetQueueAttributes", queue.url)
	checkEq("GetQueueAttributes.all_has_arn", all[attrQueueArn], queue.arn)
	checkEq("GetQueueAttributes.all_has_visibility", all[attrVisibility], "30")
	for _, name := range []string{"CreatedTimestamp", "LastModifiedTimestamp",
		attrMessages, attrNotVisible, attrDelayed} {
		check("GetQueueAttributes.all_has_"+name, all[name] != "",
			"absent; got "+joinURLs(sortedKeys(all)))
	}
	// The rig's own width, read back off a queue that never named one: this is
	// the facade's QUEEN_SQS_DEFAULT_PARTITIONS, and the suite knows what it
	// should be from the same environment the stack came from.
	checkEq("GetQueueAttributes.default_partitions_are_the_rigs",
		all[attrPartitions], strconv.Itoa(r.partitions))

	selected := r.attributes(ctx, "GetQueueAttributes.selected", queue.url, attrVisibility)
	checkEq("GetQueueAttributes.selection_is_exact", sortedKeys(selected), []string{attrVisibility})

	_, err := r.sqs.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: &queue.url, Attributes: map[string]string{attrVisibility: "45"},
	})
	if checkNoErr("SetQueueAttributes.succeeds", err) {
		after := r.attributes(ctx, "SetQueueAttributes", queue.url)
		checkEq("SetQueueAttributes.applies", after[attrVisibility], "45")
		// SetQueueAttributes MERGES: AWS has no way to remove an attribute, so
		// one that was not named must survive untouched.
		checkEq("SetQueueAttributes.merges_rather_than_replaces", after[attrRetention], "3600")
	}

	_, err = r.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &queue.url, AttributeNames: []types.QueueAttributeName{"NotAnAttribute"},
	})
	expectAPIError("GetQueueAttributes.unknown_attribute_refused", err,
		"InvalidAttributeName", new(*types.InvalidAttributeName))

	_, err = r.sqs.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: &queue.url, Attributes: map[string]string{"NotAnAttribute": "1"},
	})
	expectAPIError("SetQueueAttributes.unknown_attribute_refused", err,
		"InvalidAttributeName", new(*types.InvalidAttributeName))
}

func tQueueTags(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "TagQueue", "tags", nil, map[string]string{"team": "billing"})
	if !made {
		return
	}

	stored, err := r.sqs.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: &queue.url})
	if checkNoErr("ListQueueTags.succeeds", err) {
		checkEq("CreateQueue.tags_are_stored", stored.Tags, map[string]string{"team": "billing"})
	}

	_, err = r.sqs.TagQueue(ctx, &sqs.TagQueueInput{
		QueueUrl: &queue.url, Tags: map[string]string{"env": "rig", "owner": "compat"},
	})
	if checkNoErr("TagQueue.succeeds", err) {
		added, _ := r.sqs.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: &queue.url})
		checkEq("TagQueue.adds", added.Tags,
			map[string]string{"team": "billing", "env": "rig", "owner": "compat"})
	}

	_, err = r.sqs.TagQueue(ctx, &sqs.TagQueueInput{
		QueueUrl: &queue.url, Tags: map[string]string{"env": "rig2"},
	})
	if checkNoErr("TagQueue.overwrite_succeeds", err) {
		overwritten, _ := r.sqs.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: &queue.url})
		checkEq("TagQueue.overwrites_one_key", overwritten.Tags["env"], "rig2")
	}

	_, err = r.sqs.UntagQueue(ctx, &sqs.UntagQueueInput{
		QueueUrl: &queue.url, TagKeys: []string{"owner"},
	})
	if checkNoErr("UntagQueue.succeeds", err) {
		left, _ := r.sqs.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: &queue.url})
		checkEq("UntagQueue.removes", sortedKeys(left.Tags), []string{"env", "team"})
	}
}

func tErrors(ctx context.Context, r *rig) {
	absentName := "go-absent-" + r.run
	absentURL := r.urlOfQueue(absentName)

	_, err := r.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &absentName})
	expectAPIError("Errors.get_queue_url_on_a_missing_queue", err,
		"QueueDoesNotExist", new(*types.QueueDoesNotExist))

	_, err = r.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &absentURL})
	expectAPIError("Errors.receive_on_a_missing_queue", err,
		"QueueDoesNotExist", new(*types.QueueDoesNotExist))

	_, err = r.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &absentURL, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	expectAPIError("Errors.get_attributes_on_a_missing_queue", err,
		"QueueDoesNotExist", new(*types.QueueDoesNotExist))

	body := "nowhere"
	_, err = r.sqs.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &absentURL, MessageBody: &body})
	expectAPIError("Errors.send_to_a_missing_queue", err,
		"QueueDoesNotExist", new(*types.QueueDoesNotExist))

	// A URL from another account is a queue this deployment does not serve, and
	// must read as absent rather than as a malformed request.
	foreign := r.endpoint + "/999999999999/anything"
	_, err = r.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &foreign})
	expectAPIError("Errors.queue_url_for_another_account", err,
		"QueueDoesNotExist", new(*types.QueueDoesNotExist))

	// A wrong secret is a signature that does not verify. It must be refused as
	// a signature problem, not as a missing queue — the rig's whole auth story
	// is one static credential, so this is the only proof that it is checked.
	_, err = r.impostor().ListQueues(ctx, &sqs.ListQueuesInput{})
	code := apiCode(err)
	check("Errors.wrong_secret_is_refused",
		err != nil && (code == "SignatureDoesNotMatch" || code == "InvalidClientTokenId" ||
			code == "AccessDenied" || code == "InvalidSecurity" || code == "IncompleteSignature"),
		"got "+code)
}

func tDeleteQueue(ctx context.Context, r *rig) {
	queue, made := r.newQueue(ctx, "DeleteQueue", "gone", nil, nil)
	if !made {
		return
	}
	if _, sent := r.send(ctx, "DeleteQueue", queue.url, "doomed"); !sent {
		return
	}

	_, err := r.sqs.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: &queue.url})
	if !checkNoErr("DeleteQueue.succeeds", err) {
		return
	}
	r.forgetQueue(queue.url)

	_, err = r.sqs.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &queue.name})
	expectAPIError("DeleteQueue.url_lookup_afterwards", err,
		"QueueDoesNotExist", new(*types.QueueDoesNotExist))

	_, err = r.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &queue.url})
	expectAPIError("DeleteQueue.receive_afterwards", err,
		"QueueDoesNotExist", new(*types.QueueDoesNotExist))

	listed, err := r.sqs.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: &queue.name})
	if checkNoErr("DeleteQueue.list_succeeds", err) {
		check("DeleteQueue.is_out_of_ListQueues", len(listed.QueueUrls) == 0,
			"still listed: "+joinURLs(listed.QueueUrls))
	}

	// AWS arms a 60-second window in which the name cannot be reused, and SDK
	// retry behaviour depends on the code.
	_, err = r.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queue.name})
	expectAPIError("DeleteQueue.name_is_reserved_for_sixty_seconds", err,
		"QueueDeletedRecently", new(*types.QueueDeletedRecently))
}

// impostor is the same client with the wrong secret: same access key id, so the
// facade finds the credential and fails the signature rather than the lookup.
func (r *rig) impostor() *sqs.Client {
	cfg := aws.Config{
		Region: r.region,
		Credentials: credentials.NewStaticCredentialsProvider(
			env("AWS_ACCESS_KEY_ID", defaultAKID), "wrong-secret", ""),
		ClientLogMode: aws.LogRequest,
		Logger:        r.proto,
	}
	base := aws.String(r.endpoint)
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = base })
}

func (r *rig) forgetQueue(url string) {
	kept := r.queues[:0]
	for _, candidate := range r.queues {
		if candidate != url {
			kept = append(kept, candidate)
		}
	}
	r.queues = kept
}
