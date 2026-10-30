<template>
  <!-- A tab of the Queues section: the queue is picked in the list beside
       (?queue=), every queue's dead letters when none is. -->
  <QueuesLayout :live="refreshAgo">
    <!-- The page's one bulk write, offered once a queue is picked: purging
         "by criteria" across the whole dead letter is not offered. -->
    <template v-if="canAdmin && filterQueue.trim()" #actions>
      <button
        class="btn btn-ghost"
        :disabled="bulkPurging"
        :title="bulkPurgeButtonTitle"
        @click="openBulkPurge"
      >Purge by criteria…</button>
    </template>
  <div class="view-container dlq">

    <!-- The list failed: say so instead of drawing an empty, healthy page. -->
    <div v-if="error" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not load the dead letter</strong> · {{ describeApiError(error) }}<template v-if="messages.length"> · showing the last rows{{ lastUpdatedText ? ` (${lastUpdatedText})` : '' }}</template>
      </span>
    </div>

    <!-- Applied server-side (the endpoint takes queue + consumerGroup), so it
         selects from the whole dead letter, not from the loaded page. -->
    <PageTools>
      <div class="tool-field dlq-group">
        <label class="tool-label" for="dlq-group">Consumer group</label>
        <Autocomplete
          id="dlq-group"
          v-model="filterGroup"
          :options="groupSuggestions"
          label="Consumer group"
          placeholder="All"
          allow-custom
        />
      </div>
      <template #view>
        <!-- A broker capability, not a data fact. -->
        <span v-if="!serverPaginates" class="tool-note" title="This broker returns the whole dead letter at once">No paging on this broker</span>
        <UiSelect v-model="pageSize" label="Show" :options="PAGE_SIZES" />
      </template>
    </PageTools>

    <!-- What an operator opens this page for is WHY the messages failed: one
         row per kind of error, worst first, the text in full. With a queue
         picked the kinds are the broker's, over the queue's newest dead
         letters (composables/dlqWhy); without one, the page's. A row narrows
         the table below to its messages on this page. -->
    <div v-if="why.length" class="card">
      <div class="card-header">
        <h3>Why they failed</h3>
        <span class="card-sub tabular-nums">{{ whyScopeText }}</span>
      </div>
      <ul class="dlq-why" :aria-label="`${why.length} ${why.length === 1 ? 'kind of error' : 'kinds of error'}`">
        <li v-for="r in visibleWhy" :key="r.key">
          <button
            type="button"
            class="dlq-why-row"
            :class="{ on: errorFilter !== null && errorFilter === r.filter }"
            :disabled="r.filter === null"
            :aria-pressed="errorFilter !== null && errorFilter === r.filter"
            :title="r.filter === null ? 'No message on this page has this error' : (errorFilter === r.filter ? 'Show every message' : 'Show only these messages on this page')"
            @click="toggleErrorFilter(r.filter)"
          >
            <span class="dlq-why-share tabular-nums">{{ sharePct(r.share) }}</span>
            <span class="dlq-why-body">
              <span class="dlq-why-text">{{ r.text }}{{ r.cut ? '…' : '' }}</span>
              <span v-if="why.length > 1" class="dlq-why-bar" aria-hidden="true"><span :style="{ width: Math.max(2, r.share * 100) + '%' }" /></span>
            </span>
            <span class="dlq-why-n tabular-nums">{{ formatNumber(r.n) }}</span>
          </button>
        </li>
      </ul>
      <div v-if="why.length > COLLAPSED_ERRORS" class="dlq-why-more">
        <button type="button" class="dlq-link" @click="showAllErrors = !showAllErrors">
          {{ showAllErrors ? `Show the top ${COLLAPSED_ERRORS}` : `Show all ${why.length}` }}
        </button>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <h3>Dead-lettered messages</h3>
        <span class="card-sub tabular-nums">{{ formatNumber(pageMessages.length) }} on page {{ page }}</span>
        <!-- A narrowed table says so where its rows are counted. -->
        <button v-if="errorFilter" type="button" class="dlq-filter-chip" :title="`Showing ${messages.length}: ${errorFilter}. Click to show every row.`" @click="errorFilter = null">
          <span class="dlq-filter-chip-text">{{ formatNumber(messages.length) }} shown · {{ errorFilter }}</span>
          <Icon name="x" :size="12" />
          <span class="sr-only">Clear the error filter</span>
        </button>
        <span class="muted">{{ stamp(listPanel) }}</span>
      </div>

      <div class="dlq-scroll">
        <table class="t dlq-table">
          <thead>
            <tr>
              <th>Transaction ID</th>
              <th v-if="!filterQueue">Queue</th>
              <th>Consumer group</th>
              <th v-if="showErrorColumn">Error</th>
              <th class="r">Retries</th>
              <th class="r">Failed</th>
              <th v-if="canAdmin" class="r"><span class="sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            <!-- First paint only: a refresh or a page turn leaves the rows that
                 already loaded on screen. -->
            <template v-if="firstLoad">
              <tr v-for="i in 8" :key="i" aria-hidden="true">
                <td><div class="skeleton" style="height:14px; width:112px;" /></td>
                <td v-if="!filterQueue"><div class="skeleton" style="height:14px; width:88px;" /></td>
                <td><div class="skeleton" style="height:14px; width:104px;" /></td>
                <td v-if="showErrorColumn"><div class="skeleton" style="height:14px; width:168px;" /></td>
                <td><div class="skeleton" style="height:14px; width:24px; margin-left:auto;" /></td>
                <td><div class="skeleton" style="height:14px; width:56px; margin-left:auto;" /></td>
                <td v-if="canAdmin"><div class="skeleton" style="height:14px; width:96px; margin-left:auto;" /></td>
              </tr>
            </template>

            <template v-else-if="messages.length">
              <!-- Every row here is a failure — that is what the page is — so
                   nothing in a row is painted as one. The error text gets the
                   strongest ink: it is the column the reader came for. -->
              <tr
                v-for="msg in messages"
                :key="rowKey(msg)"
                class="dlq-row"
                :class="{ 'dlq-row-on': selectedKey && rowKey(msg) === selectedKey }"
                tabindex="0"
                @click="selectMessage(msg)"
                @keydown.enter.prevent="selectMessage(msg)"
              >
                <td><span class="dlq-txn" :title="msg.transactionId || msg.id">{{ midCut(msg.transactionId || msg.id || '—') }}</span></td>
                <td v-if="!filterQueue"><span class="dlq-cut dlq-queue" :title="msg.queue">{{ msg.queue || '—' }}</span></td>
                <td><span class="dlq-cut dlq-group-name" :title="msg.consumerGroup">{{ msg.consumerGroup || '—' }}</span></td>
                <td v-if="showErrorColumn"><span class="dlq-cut dlq-err-cell" :title="msg.errorMessage">{{ msg.errorMessage || '—' }}</span></td>
                <td class="r dlq-num">{{ msg.retryCount ?? '—' }}</td>
                <td class="r dlq-when" :title="formatTimestampUtc(msg.failedAt)">{{ formatRelativeTime(msg.failedAt) }}</td>
                <td v-if="canAdmin" class="r dlq-actions" @click.stop @keydown.enter.stop>
                  <!-- Replay before Purge: the recoverable action reads first.
                       A row whose listing carries no id cannot be addressed by
                       the move route, so its button says why. -->
                  <button
                    class="btn btn-ghost dlq-row-btn"
                    :disabled="!msg.id || isReplaying(msg) || replayUnavailable"
                    :title="replayUnavailable
                      ? 'This cell does not serve the replay route: the broker predates it, or the proxy in front does not classify it'
                      : (msg.id
                        ? `Replay this message onto ${msg.queue || 'its queue'}`
                        : 'This broker’s dead-letter list carries no row id, so a replay cannot address this row')"
                    @click="openReplay(msg)"
                  >{{ isReplaying(msg) ? '…' : 'Replay' }}</button>
                  <button class="btn btn-ghost dlq-row-btn dlq-row-purge" :disabled="isDeleting(msg)" @click="askPurge(msg)">{{ isDeleting(msg) ? '…' : 'Purge' }}</button>
                  <!-- A purge the broker refused stays on screen with its reason. -->
                  <div v-if="rowError(msg)" class="dlq-row-err">{{ rowError(msg) }}</div>
                </td>
              </tr>
            </template>

            <!-- Never an idle empty state on a failure. -->
            <tr v-else-if="error">
              <td :colspan="columns">
                <div class="empty-state empty-state-failed">
                  <h3>Cannot load the dead letter</h3>
                  <button class="btn btn-ghost" @click="fetchMessages">Retry</button>
                </div>
              </td>
            </tr>

            <tr v-else>
              <td :colspan="columns">
                <div class="empty-state">
                  <Icon name="archive-x" class="empty-state-icon" :size="28" />
                  <h3>{{ page > 1 ? 'No more dead letters' : hasFilters ? 'No dead letter matches' : 'No dead letters' }}</h3>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Not drawn under an empty first page, a failure, or a broker that
           ignores the limit: Previous and Next there go nowhere. -->
      <div v-if="serverPaginates && (pageMessages.length || page > 1)" class="pager">
        <span class="pager-count">Page <span class="tabular-nums">{{ page }}</span></span>
        <div class="pager-nav">
          <button class="btn btn-ghost" :disabled="page === 1" @click="prevPage">Previous</button>
          <button class="btn btn-ghost" :disabled="!canPageForward" @click="nextPage">Next</button>
        </div>
      </div>
    </div>

    <Teleport to="body">
      <div v-if="bulkTarget" class="modal-backdrop" @click.self="closeBulkPurge">
        <div class="card modal-card">
          <div class="card-header"><h3>Purge by criteria</h3></div>
          <div class="card-body dlq-form">
            <div v-if="bulkPurgeError" class="panel-err">{{ bulkPurgeError }}</div>
            <p class="dlq-lead">
              Purge every dead-letter record of <strong>{{ bulkTarget.queue }}</strong><template v-if="bulkTarget.consumerGroup"> for <strong>{{ bulkTarget.consumerGroup }}</strong></template>?
            </p>
            <p class="dlq-help" title="The error filter narrows only the loaded page and is not part of this purge.">Every record, not only this page’s. This cannot be undone.</p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" :disabled="bulkPurging" @click="closeBulkPurge">Cancel</button>
            <button class="btn btn-danger" :disabled="bulkPurging" @click="purgeByCriteria">{{ bulkPurging ? 'Purging…' : 'Purge all' }}</button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- One message's purge: from a row or from the drawer, so it sits above
         the drawer. The call takes every consumer group's record of the
         message (delete_message_v1), and the modal says so. -->
    <Teleport to="body">
      <div v-if="purgeTarget" class="modal-backdrop dlq-replay-over" @click.self="purgeTarget = null">
        <div class="card modal-card">
          <div class="card-header">
            <h3>Purge</h3>
            <span class="card-sub font-mono dlq-cut dlq-purge-id">{{ purgeTarget.transactionId || purgeTarget.id }}</span>
          </div>
          <div class="card-body dlq-form">
            <p class="dlq-lead">Purge this dead letter<template v-if="purgeTarget.queue"> of <strong>{{ purgeTarget.queue }}</strong></template>?</p>
            <p class="dlq-help">Every consumer group’s record of the message goes. This cannot be undone.</p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" @click="purgeTarget = null">Cancel</button>
            <button class="btn btn-danger" @click="confirmPurge">Purge</button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- Replay. A confirm, because a replay is a WRITE: it appends a copy to a
         partition and removes the dead-letter record, both irreversible. The
         modal names the target before the click. -->
    <Teleport to="body">
      <div v-if="replayRow" class="modal-backdrop dlq-replay-over" @click.self="closeReplay">
        <form class="card modal-card dlq-replay-card" @submit.prevent="submitReplay">
          <div class="card-header">
            <h3>{{ replayTarget.moved ? 'Move and replay' : 'Replay' }}</h3>
            <span class="card-sub font-mono">{{ replayRow.consumerGroup || 'no consumer group' }}</span>
          </div>

          <div class="card-body dlq-replay-body">
            <!-- The verdict, and the only thing allowed to call a replay a
                 success. A refusal leaves the fields as they were, to correct
                 and send again; a verdict about a FIELD is drawn under it. -->
            <div
              v-if="replayResult && !replayResult.field"
              class="dlq-verdict"
              :class="`dlq-verdict-${replayResult.kind}`"
            >
              <strong>{{ replayResult.title }}</strong>
              <p>{{ replayResult.detail }}</p>
              <router-link
                v-if="replayResult.target"
                class="btn btn-ghost dlq-verdict-link"
                :to="queueLocation(replayResult.target.queue, route, 'messages', { partition: replayResult.target.partition, from: undefined, to: undefined, range: '1h' })"
              >Open {{ replayResult.target.queue }} in Messages</router-link>
            </div>

            <!-- Gone once the row is verified gone: a live form under a
                 finished verdict invites a second attempt at nothing. -->
            <template v-if="!replayDone">
              <p class="dlq-replay-lead">
                Appends a copy to
                <strong class="font-mono">{{ replayTarget.queue }}</strong><span class="dlq-replay-sep">/</span><strong class="font-mono">{{ replayTarget.partition }}</strong>
                as <strong class="font-mono">{{ replayTarget.transactionId }}</strong><template v-if="!replayTarget.namesDestination">, where it failed</template>.
              </p>

              <ul class="dlq-replay-facts">
                <li title="One move, not a push followed by a cleanup that can fail on its own. If nothing is written (the destination already holds this transaction id), nothing is removed either.">The dead-letter record is removed in the same step.</li>
                <li>Other consumer groups’ records of this message stay.</li>
                <li>The copy goes to the tail; its age restarts there.</li>
                <li v-if="replayTarget.moved">
                  A move: it failed on <strong class="font-mono">{{ replayRow.queue }}/{{ replayRow.partition }}</strong>. A destination that does not exist yet is created with default options.
                </li>
                <li
                  v-if="replayTarget.namesDestination"
                  title="Broker-direct, an omitted half falls back to this row's own value without saying so; a cloud proxy refuses a half-named destination. The whole pair means the same in both."
                >The request names queue and partition together.</li>
              </ul>

              <!-- The destination override, behind a fold: replaying where it
                   failed is the answer nearly every time. -->
              <button type="button" class="dlq-toggle" :aria-expanded="showReplayAdvanced ? 'true' : 'false'" @click="showReplayAdvanced = !showReplayAdvanced">
                <Icon name="chevron-right" class="dlq-toggle-chev" :class="{ 'dlq-toggle-open': showReplayAdvanced }" :size="14" />
                Replay somewhere else
                <span v-if="replayTarget.moved" class="detail-note dlq-toggle-badge">· destination changed</span>
              </button>

              <div v-if="showReplayAdvanced" class="dlq-replay-advanced">
                <div class="dlq-replay-field">
                  <label class="dlq-field-label" for="dlq-target-queue">Queue</label>
                  <Autocomplete
                    id="dlq-target-queue"
                    v-model="replayForm.queue"
                    :options="queueOptions"
                    :loading="queuesLoading"
                    label="Queue"
                    :placeholder="replayRow.queue || 'Where it failed'"
                    allow-custom
                  />
                  <span v-if="replayResult?.field === 'queue'" class="dlq-invalid">{{ replayResult.detail }}</span>
                  <span
                    v-else
                    class="dlq-help"
                    title="Pick a name, or press Enter to accept one this cluster does not carry yet. The sentence above always names the destination this button writes to."
                  >Blank keeps <span class="font-mono">{{ replayRow.queue || 'the queue it failed on' }}</span>.</span>
                </div>

                <label class="dlq-replay-field">
                  <span class="dlq-field-label">Partition</span>
                  <input
                    v-model="replayForm.partition"
                    class="input font-mono"
                    autocomplete="off"
                    spellcheck="false"
                    :placeholder="replayRow.partition || 'Where it failed'"
                  />
                  <span v-if="replayResult?.field === 'partition'" class="dlq-invalid">{{ replayResult.detail }}</span>
                  <span
                    v-else
                    class="dlq-help"
                    title="Fill either field and the request names the whole pair."
                  >Blank keeps <span class="font-mono">{{ replayRow.partition || 'the partition it failed on' }}</span>.</span>
                </label>
              </div>
            </template>
          </div>

          <div class="modal-foot">
            <template v-if="replayDone">
              <button type="button" class="btn btn-primary" @click="closeReplay">Done</button>
            </template>
            <template v-else>
              <button type="button" class="btn btn-ghost" :disabled="replaying" @click="closeReplay">Cancel</button>
              <!-- Disabled in flight; the broker's answer is what removes a
                   row here, never the click that asked for it. -->
              <button type="submit" class="btn btn-primary" :disabled="replaying || !replayTarget.id">
                {{ replaying ? 'Replaying…' : (replayTarget.moved ? 'Move and replay' : 'Replay') }}
              </button>
            </template>
          </div>
        </form>
      </div>
    </Teleport>

    <DetailDrawer
      :open="Boolean(selectedMsg)"
      title="Dead letter"
      :subtitle="selectedMsg?.transactionId || selectedMsg?.id || ''"
      wide
      split
      @close="closeDetail"
    >
      <template #actions>
        <button
          class="btn btn-ghost"
          title="Copy the failure, routing, times and payload as Markdown"
          @click="copyMarkdown"
        >{{ markdownCopied ? 'Copied' : 'Copy as Markdown' }}</button>
      </template>

      <template v-if="selectedMsg">
        <!-- In words, never red: a dead letter is a message the broker has
             already given up on and parked, history rather than a failure now. -->
        <div class="detail-status-row">
          <span class="detail-status">Dead-lettered</span>
          <span v-if="selectedMsg.retryCount" class="detail-note">· after {{ selectedMsg.retryCount }} {{ selectedMsg.retryCount === 1 ? 'retry' : 'retries' }}</span>
        </div>

        <div class="detail-fields">
          <DetailField label="Queue" :value="selectedMsg.queue" tone="high" />
          <DetailField label="Partition" :value="selectedMsg.partition" mono copyable />
          <DetailField label="Partition ID" :value="selectedMsg.partitionId" mono copyable />
          <DetailField label="Transaction ID" :value="selectedMsg.transactionId" mono copyable />
          <DetailField label="Consumer group" :value="selectedMsg.consumerGroup" mono tone="accent" />

          <!-- The broker does not return a dead letter's enqueue time: it
               echoes failed_at as createdAt, and the same instant twice under
               two labels would invent a fact. -->
          <DetailField
            v-if="hasDistinctCreatedAt(selectedMsg)"
            label="Created"
            :value="formatTimestamp(selectedMsg.createdAt)"
            :title="formatTimestampUtc(selectedMsg.createdAt)"
          />
          <DetailField
            label="Failed at"
            :value="formatTimestamp(selectedMsg.failedAt)"
            :title="formatTimestampUtc(selectedMsg.failedAt)"
          >
            <span v-if="!hasDistinctCreatedAt(selectedMsg)" class="detail-note">Enqueue time not recorded.</span>
          </DetailField>
        </div>

        <DetailField
          v-if="selectedMsg.errorMessage"
          class="detail-section"
          label="Error"
          :value="selectedMsg.errorMessage"
          mono
          copyable
          boxed
          tone="high"
        />

        <!-- Replay moves this snapshot on the broker's move primitive (lock,
             push, delete, one transaction); purge is the end of the line. -->
        <div v-if="canAdmin" class="detail-actions">
          <div class="detail-buttons">
            <button
              class="btn"
              :disabled="!selectedMsg.id || isReplaying(selectedMsg) || replayUnavailable"
              :title="replayUnavailable
                ? 'This cell does not serve the replay route: the broker predates it, or the proxy in front does not classify it'
                : (selectedMsg.id
                  ? null
                  : 'This broker’s dead-letter list carries no row id, so a replay cannot address this row')"
              @click="openReplay(selectedMsg)"
            >{{ isReplaying(selectedMsg) ? 'Replaying…' : 'Replay' }}</button>
            <button
              class="btn btn-danger"
              :disabled="isDeleting(selectedMsg)"
              @click="askPurge(selectedMsg)"
            >{{ isDeleting(selectedMsg) ? 'Purging…' : 'Purge' }}</button>
          </div>
          <p v-if="rowError(selectedMsg)" class="detail-note is-bad">{{ rowError(selectedMsg) }}</p>
        </div>
        <p v-else class="detail-actions detail-note">Replaying and purging need the admin role on this cluster.</p>
      </template>

      <template #secondary>
        <div v-if="selectedMsg?.data !== undefined">
          <div class="detail-section-header">
            <label class="label-xs">Payload</label>
            <button class="btn btn-ghost detail-copy-button" @click="copyPayload">
              {{ copied ? 'Copied' : (encryptedPayload ? 'Copy envelope' : 'Copy') }}
            </button>
          </div>
          <!-- The dead-letter read path does no decryption: what follows is the
               stored envelope, not the message. -->
          <p
            v-if="encryptedPayload"
            class="dlq-quiet"
            title="This queue encrypts payloads, and the dead-letter endpoint returns them as stored. This is ciphertext, not the message body."
          >Encrypted: this is the stored envelope, not the message.</p>
          <JsonViewer :value="selectedMsg.data" />
        </div>
      </template>
    </DetailDrawer>
  </div>
  </QueuesLayout>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { useRoute } from 'vue-router'
import { useRouteState } from '@/composables/useRouteState'
import { queueLocation } from '@/composables/navigation'
const route = useRoute()
import { dlq, queues as queuesApi, system, describeApiError } from '@/api'
import { midCut, whyRows, whyScope } from '@/composables/dlqWhy'
import { useApi, formatNumber, formatRelativeTime } from '@/composables/useApi'
import { formatDlqMarkdown } from '@/composables/useDlqMarkdown'
import { dlqRowKey, replayRequest, replayVerdict } from '@/composables/useDlqReplay'
import { formatTimestamp, formatTimestampUtc } from '@/composables/useFormat'
import { useRefresh } from '@/composables/useRefresh'
import { stamp } from '@/composables/useStamp'
import { useToast } from '@/composables/useToast'
import { currentEpoch, useIdentity } from '@/stores/identity'
import { routeSupport } from '@/stores/routeSupport'
import Autocomplete from '@/components/Autocomplete.vue'
import QueuesLayout from '@/components/QueuesLayout.vue'
import PageTools from '@/components/PageTools.vue'
import { useRefreshAgo } from '@/composables/useRefreshAgo'
import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import JsonViewer from '@/components/JsonViewer.vue'
import Icon from '@/components/Icon.vue'
import UiSelect from '@/components/UiSelect.vue'
import { useGroupsStore } from '@/stores/groupsStore'

const { can, epoch, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
const { notifySuccess, notifyInfo, notifyWarn, notifyError } = useToast()

/** Kinds of error shown before "Why they failed" has to be expanded. */
const COLLAPSED_ERRORS = 6
/** Errors the broker sent with no text: still a group, and still says so. */
const NO_ERROR_TEXT = '(no error text recorded)'

const errorKey = (msg) => msg.errorMessage || NO_ERROR_TEXT
const PAGE_SIZES = [25, 50, 100, 200]

const selectedKey = ref(null)
const copied = ref(false)
const markdownCopied = ref(false)
// Client-side narrowing of the loaded page by one error text. Not a request
// parameter: the DLQ endpoint takes queue and consumerGroup only.
const errorFilter = ref(null)
const showAllErrors = ref(false)
const filterQueue = ref('')
const filterGroup = ref('')
const page = ref(1)
const pageSize = ref(25)
const { restoring: restoringRoute } = useRouteState({ queue: filterQueue, group: filterGroup, page, limit: pageSize })
// Flipped off when a response carries more rows than it was asked for: that
// broker is not applying the limit, so offering "Next" would page nothing.
const serverPaginates = ref(true)
// Per-row transient state, keyed rather than stamped onto the row objects,
// which are replaced wholesale on every refresh.
const deleting = ref(new Set())
const rowErrors = ref(new Map())
const bulkTarget = ref(null)
const bulkPurging = ref(false)
const bulkPurgeError = ref(null)
// Rows this session verified as purged (success:true). Cleared on every reload,
// so a row that comes back is a row the broker still has.
const purged = ref(new Set())

// ---------------------------------------------------------------------------
// Replay (PLAN_DASHBOARD_ACTIONS.md §2.3). One row at a time: the confirm modal
// IS the in-flight state, so there is no per-row set to keep the way `deleting`
// has to for a purge that fires straight off the row.
// ---------------------------------------------------------------------------
const replayRow = ref(null)
const replayForm = ref({ queue: '', partition: '' })
const showReplayAdvanced = ref(false)
const replaying = ref(false)
const replayResult = ref(null)
// The request that is on the wire, captured at submit. While it is set it — and
// not the live Advanced inputs — is what the modal names: the sentence and the
// title must describe the destination this request is writing to, not the one
// somebody is typing over it while it runs.
const replayInFlight = ref(null)
// §1.6 / §3 rule 5: a route family is probed ONCE per cluster epoch. The
// verdict cannot be read off the status code here — `gone` is a 404 too — so it
// comes from the body-based mapper (`unavailable`), and only that one answer
// takes the affordance away.
const REPLAY_ROUTE = 'dlq-replay'
// Read through `epoch` so a cluster switch re-asks: the store keys its verdicts
// by the epoch too, and a cell that lacks the route says nothing about the next
// one. `replayProbed` is this component's re-render trigger — the store is a
// plain Map and cannot be one.
const replayProbed = ref(0)
const replayUnavailable = computed(() => {
  void replayProbed.value
  void epoch.value
  return Boolean(routeSupport.missing(REPLAY_ROUTE))
})
// Rows this session watched leave queen.log_dlq — moved, or found already gone.
// NOT a `duplicate`: that verdict writes nothing and therefore removes nothing,
// so the record is still dead-lettered and the row stays on screen. Cleared on
// every reload like `purged`, so a row that comes back is a row the broker
// still has.
//
// Keyed by the DLQ ROW ID, not by this page's `msgKey`. One transaction id can
// carry a dead-letter row PER CONSUMER GROUP and the move removes exactly the
// row it addressed, so dropping every row that shares the transaction id would
// report the other groups' records as replayed as well — which is precisely
// the defect the old retry route had (§1.3). `purged` can stay on `msgKey`
// because `delete_message_v1` really does delete every row for the address.
const replayed = ref(new Set())

/**
 * The identity of ONE dead-letter row — what the table keys, what the drawer
 * selects, and what the replay path addresses. The rule and its fallback live
 * in the composable, next to the route that takes the id (and its tests).
 */
const rowKey = dlqRowKey

/**
 * The MESSAGE's address, which is a DIFFERENT thing and names more rows: a
 * transaction id carries one dead-letter record per consumer group, and
 * `delete_message_v1` deletes all of them in one call. So this is kept for
 * exactly the surfaces where those all-groups semantics are the truth — the
 * purge's in-flight flag, its error line and its suppression set — and for
 * nothing that has to name the one row an operator clicked.
 */
const msgKey = (msg) => msg.transactionId || msg.id

// The list, its loading/error state and its "as of when" come from one place.
// useApi also aborts on unmount and drops any response belonging to a cluster
// we have since left — the previous tenant's DLQ must never land here.
// The panel object itself is kept so every card header can be stamped from it.
const listPanel = useApi((params, config) => dlq.list(params, config), {
  immediate: false,
  onSuccess: (payload) => {
    const rows = extractRows(payload)
    serverPaginates.value = rows.length <= pageSize.value
    purged.value = new Set()
    replayed.value = new Set()
    rowErrors.value = new Map()
    // Drop a selection the new page cannot honour, so the table is never
    // narrowed by an error none of its rows carry.
    if (errorFilter.value && !rows.some(m => errorKey(m) === errorFilter.value)) {
      errorFilter.value = null
    }
    // A detail panel over a row that is no longer listed is a stale fact.
    if (selectedKey.value && !rows.some(m => rowKey(m) === selectedKey.value)) {
      selectedKey.value = null
    }
  },
})
const refreshAgo = useRefreshAgo(listPanel.lastUpdated)

const {
  data: listData,
  loading,
  error,
  lastUpdated,
  execute: executeList,
} = listPanel

const {
  data: queuesData,
  loading: queuesLoading,
  refresh: refreshQueues,
} = useApi((config) => queuesApi.list(undefined, config), { immediate: false })

const extractRows = (payload) =>
  Array.isArray(payload?.messages) ? payload.messages : (Array.isArray(payload) ? payload : [])

/** Everything the broker returned for this page, minus the rows this session
    watched leave the dead-letter queue — purged, or replayed out of it. */
const pageMessages = computed(
  () => extractRows(listData.value)
    .filter(m => !purged.value.has(msgKey(m)) && !replayed.value.has(m.id))
)
/** What the table shows: the page, narrowed by the breakdown's error filter. */
const messages = computed(() => (
  errorFilter.value
    ? pageMessages.value.filter(m => errorKey(m) === errorFilter.value)
    : pageMessages.value
))
const queueOptions = computed(
  () => (queuesData.value?.queues || []).map(q => q.name).filter(Boolean).sort()
)
// Resolved against the whole page, not the filtered view: narrowing the table
// must not empty a drawer the user already has open. By row identity, never by
// the message address — two consumer groups' records for the same transaction
// id are two rows on this page, and the drawer carries a Replay button that
// moves whichever one it resolved.
const selectedMsg = computed(() => (
  selectedKey.value
    ? pageMessages.value.find(m => rowKey(m) === selectedKey.value) || null
    : null
))

/** Skeletons on the first paint only: a refresh leaves the rows on screen. */
const firstLoad = computed(() => loading.value && !listData.value)

const isDeleting = (msg) => deleting.value.has(msgKey(msg))
const rowError = (msg) => rowErrors.value.get(msgKey(msg)) || null
/** In flight for THIS row: the modal only ever holds one, and it is addressed
    by the DLQ row id — two consumer groups' rows for the same transaction id
    are two separate replays and only one of them is running. */
const isReplaying = (msg) =>
  replaying.value && Boolean(replayRow.value?.id) && replayRow.value.id === msg.id

const canAdmin = computed(() => can('queueAdmin'))

/** What the replay will send and where it will land, as the modal states it.
    Derived from the row and the two override fields, never from either alone —
    except while a request is in flight, when it IS that request. */
const replayTarget = computed(
  () => replayInFlight.value || replayRequest(replayRow.value || {}, replayForm.value)
)
/** The dead-letter row is verified gone, so there is nothing left to send. */
const replayDone = computed(() => Boolean(replayResult.value?.removeRow))

const bulkPurgeButtonTitle = computed(() => (
  filterGroup.value.trim()
    ? `Purge all DLQ records for ${filterQueue.value.trim()} and consumer group ${filterGroup.value.trim()}`
    : `Purge all DLQ records for ${filterQueue.value.trim()}`
))

// ---------------------------------------------------------------------------
// Why they failed — the page's kinds of error, and the table's filter
// ---------------------------------------------------------------------------

/** Every group is computed over the PAGE, never over the filtered view: a
    breakdown that narrows with its own selection cannot be clicked back open. */
const errorGroups = computed(() => {
  const counts = new Map()
  for (const m of pageMessages.value) {
    const key = errorKey(m)
    counts.set(key, (counts.get(key) || 0) + 1)
  }
  const total = pageMessages.value.length || 1
  return [...counts.entries()]
    .map(([error, count]) => ({ error, count, share: count / total }))
    .sort((a, b) => b.count - a.count || a.error.localeCompare(b.error))
})

// Why the picked queue's messages failed: the broker folds the errors of its
// newest dead letters into signatures (one request, refreshed with the page).
// Quiet: a broker without the route keeps the page's own breakdown.
const whyPanel = useApi((params, config) => system.getDlqSignatures(params, { ...config, probe: true, quiet: true }), { immediate: false })
const whyAnswer = computed(() => {
  const d = whyPanel.data.value
  return d && filterQueue.value && d.queue === filterQueue.value ? d : null
})
const fetchWhy = () => (filterQueue.value
  ? whyPanel.execute({ queue: filterQueue.value }).catch(() => {})
  : Promise.resolve())
const why = computed(() => (pageMessages.value.length || whyAnswer.value
  ? whyRows(whyAnswer.value, errorGroups.value, pageMessages.value)
  : []))
const visibleWhy = computed(() => (showAllErrors.value ? why.value : why.value.slice(0, COLLAPSED_ERRORS)))
const clock = (iso) => {
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return '—'
  const sameDay = d.toDateString() === new Date().toDateString()
  return sameDay
    ? d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    : d.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
}
const whyScopeText = computed(() => whyScope(whyAnswer.value, pageMessages.value.length, clock))
const sharePct = (share) => {
  const p = (Number(share) || 0) * 100
  return p >= 99.5 ? '100%' : p < 1 ? '<1%' : `${Math.round(p)}%`
}

/** Toggle, not set: a second click on the active error clears the filter. */
const toggleErrorFilter = (error) => {
  if (error === null || error === undefined) return
  errorFilter.value = errorFilter.value === error ? null : error
}

// The Error column says something only when the rows on screen differ in it:
// one kind of error is said once, in "Why they failed".
const showErrorColumn = computed(() => !errorFilter.value && errorGroups.value.length > 1)

// The groups to offer: the picked queue's from the app's one group listing
// (every queue's when none is picked), and any this page shows.
const groupsStore = useGroupsStore()
const groupSuggestions = computed(() => {
  const queue = filterQueue.value.trim()
  const known = (groupsStore.groups.value || [])
    .filter(g => g?.name && (!queue || g.queueName === queue))
    .map(g => g.name)
  return [...new Set([...known, ...pageMessages.value.map(m => m.consumerGroup)].filter(Boolean))].sort()
})

const hasFilters = computed(() => Boolean(filterQueue.value || filterGroup.value || errorFilter.value))
// The Queue column names what the list beside has not picked.
const columns = computed(() => 4 + (showErrorColumn.value ? 1 : 0) + (filterQueue.value ? 0 : 1) + (canAdmin.value ? 1 : 0))
// Against the page, not the filtered view: an error filter narrows what is on
// screen, it does not tell us the broker has no further page.
const canPageForward = computed(
  () => serverPaginates.value && pageMessages.value.length >= pageSize.value
)
const lastUpdatedText = computed(() =>
  lastUpdated.value ? formatRelativeTime(lastUpdated.value) : null
)

const encryptedPayload = computed(() => isEncryptedEnvelope(selectedMsg.value?.data))

/** The shape server/src/encryption.rs writes; the DLQ read path never unwraps it. */
const isEncryptedEnvelope = (data) =>
  Boolean(data) && typeof data === 'object' &&
  typeof data.encrypted === 'string' &&
  typeof data.iv === 'string' &&
  typeof data.authTag === 'string'

const hasDistinctCreatedAt = (msg) =>
  Boolean(msg.createdAt) && msg.createdAt !== msg.failedAt

const selectMessage = (msg) => {
  const key = rowKey(msg)
  selectedKey.value = key && selectedKey.value !== key ? key : null
  copied.value = false
  markdownCopied.value = false
}

const closeDetail = () => { selectedKey.value = null }

const writeClipboard = async (text) => {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    notifyError('Could not copy to the clipboard', 'Copy failed')
    return false
  }
}

const copyPayload = async () => {
  if (selectedMsg.value?.data === undefined) return
  if (await writeClipboard(JSON.stringify(selectedMsg.value.data, null, 2))) {
    copied.value = true
    setTimeout(() => { copied.value = false }, 2000)
  }
}

const copyMarkdown = async () => {
  if (!selectedMsg.value) return
  const markdown = formatDlqMarkdown(selectedMsg.value, {
    tenant: actingTenantSlug.value,
    cluster: actingClusterSlug.value,
    cell: actingCellSlug.value,
    encryptedEnvelope: encryptedPayload.value,
  })
  if (await writeClipboard(markdown)) {
    markdownCopied.value = true
    setTimeout(() => { markdownCopied.value = false }, 2000)
  }
}

// A failed load keeps whatever loaded last; the banner above says it is stale.
// The failure itself is already on the global surface (shared HTTP client).
const fetchMessages = () => {
  const params = { limit: pageSize.value, offset: (page.value - 1) * pageSize.value }
  if (filterQueue.value) params.queue = filterQueue.value
  if (filterGroup.value) params.consumerGroup = filterGroup.value
  return executeList(params).catch(() => {})
}

const reload = () => {
  page.value = 1
  // An operator reloading the page is an operator asking again: drop the
  // remembered "no replay route here" so an upgraded cell is re-probed by the
  // next click instead of staying greyed out until the tab is reopened.
  routeSupport.forget(REPLAY_ROUTE)
  replayProbed.value += 1
  fetchMessages()
}

const prevPage = () => {
  if (page.value === 1) return
  page.value--
  fetchMessages()
}

const nextPage = () => {
  if (!canPageForward.value) return
  page.value++
  fetchMessages()
}

// The confirm is the page's own modal, not the browser's: the row (or the
// drawer) asks, the modal's button purges.
const purgeTarget = ref(null)
const askPurge = (msg) => { if (canAdmin.value && msg) purgeTarget.value = msg }
const confirmPurge = () => {
  const msg = purgeTarget.value
  purgeTarget.value = null
  if (msg) purge(msg)
}

const purge = async (msg) => {
  if (!canAdmin.value) return
  const key = msgKey(msg)
  deleting.value.add(key)
  rowErrors.value.delete(key)
  try {
    const res = await dlq.delete(msg.partitionId, msg.transactionId)
    // The broker answers 200 {success:false} both when nothing matched and when
    // the ownership gate refused the partition. Dropping the row on that reports
    // a purge that never happened — and the row returns on the next poll.
    if (res.data?.success !== true) {
      const reason = res.data?.message || 'The broker did not purge this message.'
      rowErrors.value.set(key, reason)
      notifyError(reason, 'Purge failed')
      return
    }
    notifySuccess(`Purged ${key}`)
    // The purge takes every consumer group's record under this address, so a
    // drawer open on ANY of them is showing a row that no longer exists — which
    // is why this one closes on the message address and not on the row
    // identity. Read before `purged` swallows the rows it resolves from.
    if (selectedMsg.value && msgKey(selectedMsg.value) === key) selectedKey.value = null
    purged.value.add(key)
  } catch (err) {
    rowErrors.value.set(key, describeApiError(err))
  } finally {
    deleting.value.delete(key)
  }
}

const openBulkPurge = () => {
  if (!canAdmin.value) return
  const queue = filterQueue.value.trim()
  if (!queue) return
  bulkPurgeError.value = null
  bulkTarget.value = {
    queue,
    consumerGroup: filterGroup.value.trim() || null,
  }
}

const closeBulkPurge = () => {
  if (bulkPurging.value) return
  bulkTarget.value = null
  bulkPurgeError.value = null
}

const purgeByCriteria = async () => {
  if (!bulkTarget.value || bulkPurging.value) return
  const target = { ...bulkTarget.value }
  const params = { queue: target.queue }
  if (target.consumerGroup) params.consumerGroup = target.consumerGroup
  bulkPurging.value = true
  bulkPurgeError.value = null
  try {
    const res = await dlq.purge(params)
    if (res.data?.success !== true || !Number.isInteger(res.data?.deleted)) {
      throw new Error(res.data?.message || 'The broker did not confirm the bulk purge.')
    }
    const deleted = res.data.deleted
    bulkTarget.value = null
    page.value = 1
    errorFilter.value = null
    await fetchMessages()
    notifySuccess(
      deleted === 1 ? 'Purged 1 DLQ record' : `Purged ${formatNumber(deleted)} DLQ records`,
    )
  } catch (err) {
    bulkPurgeError.value = describeApiError(err)
  } finally {
    bulkPurging.value = false
  }
}

// ---------------------------------------------------------------------------
// Replay — the move primitive, addressed by the DLQ row id
// ---------------------------------------------------------------------------

const openReplay = (msg) => {
  // `id` is the address the move route takes. Without it there is nothing to
  // replay, and the two buttons are disabled with a title that says why — as
  // they are on a cell that does not serve the route at all.
  if (!canAdmin.value || !msg?.id || replayUnavailable.value) return
  replayRow.value = msg
  // Every opening starts from the row, never from what the last one left
  // behind: a target queue carried over from another message is how a replay
  // lands somewhere nobody asked for.
  replayForm.value = { queue: '', partition: '' }
  showReplayAdvanced.value = false
  replayResult.value = null
}

const closeReplay = () => {
  if (replaying.value) return
  replayRow.value = null
  replayResult.value = null
}

/** Render one answer, and act on it exactly as far as it is verified. */
const applyReplayVerdict = (rowId, verdict, epochAtStart) => {
  // An answer that belongs to a cluster we have since left describes a row that
  // is not on screen any more. Drop it, modal and all — reporting it here would
  // attribute one tenant's outcome to another.
  if (epochAtStart !== currentEpoch()) {
    replayRow.value = null
    replayResult.value = null
    return
  }

  replayResult.value = verdict

  // A refused destination renders under the field that caused it, so the fold
  // holding that field has to be open for the sentence to be readable.
  if (verdict.field) showReplayAdvanced.value = true

  // The ONLY thing that removes a row: the broker having said the dead-letter
  // record is gone (moved, duplicate, or already gone before we asked). A toast
  // goes with it, because a row leaving the table is a thing the shell cannot
  // see and the operator has to be able to read after the modal is closed.
  // Nothing else toasts: the verdict block carries the sentence where the
  // action was taken, and every non-2xx is already on the global surface.
  if (verdict.removeRow) {
    // Close the drawer BEFORE the row leaves the page, so the panel is not left
    // resolving a row that `replayed` has just filtered away — with a live
    // Replay/Purge pair still pointed at it. By row id, which is the identity
    // the move addressed: a sibling group's record for the same transaction id
    // is a different row and it is still dead-lettered.
    if (selectedMsg.value?.id === rowId) selectedKey.value = null
    replayed.value.add(rowId)
    const where = verdict.target?.transactionId ? `Transaction ${verdict.target.transactionId}` : null
    if (verdict.kind === 'success') notifySuccess(verdict.title, where)
    else if (verdict.kind === 'info') notifyInfo(verdict.title, where)
    else notifyWarn(verdict.title, 'The dead-letter list has been reloaded')
  }

  // Someone else changed this page under us (another operator, the sweeper), or
  // we never learned the outcome. Either way what is on screen predates the
  // answer, so re-read it rather than patching one row.
  if (verdict.refresh) fetchMessages()
}

const submitReplay = async () => {
  if (!replayRow.value || replaying.value) return
  const request = replayTarget.value
  if (!request.id) return

  const epochAtStart = currentEpoch()
  replaying.value = true
  // Frozen for the duration: `replayTarget` reads this, so the modal keeps
  // naming the destination this request carries even if the Advanced inputs
  // are edited under it.
  replayInFlight.value = request
  replayResult.value = null
  try {
    // `probe: true` keeps the 404s off the global toast surface, and nothing
    // else: the client suppresses the report only for the missing-route family
    // (api/httpClient.js `fail`), which here is precisely the two answers this
    // page renders itself — the row was already replayed or purged, and the
    // route is not served on this cell. A 403, a 429 or a 5xx still toasts.
    const res = await dlq.replay(request.id, request.body, { probe: true })
    applyReplayVerdict(request.id, replayVerdict(res), epochAtStart)
  } catch (err) {
    const verdict = replayVerdict(err)
    // The one stable "not here" answer, remembered for the cluster epoch so no
    // other surface re-asks and the buttons stop offering what this cell cannot
    // do. Deliberately NOT `routeSupport.guard`: it keys off any missing-route
    // error, and an already-purged row's `gone` is a 404 — guarding the call
    // would disable replay for the rest of the session after one stale row.
    if (verdict.unavailable) {
      routeSupport.remember(REPLAY_ROUTE, err)
      replayProbed.value += 1
    }
    applyReplayVerdict(request.id, verdict, epochAtStart)
  } finally {
    replaying.value = false
    replayInFlight.value = null
  }
}

// One shared ticker for the whole app (paused while the tab is hidden) instead
// of a private setInterval: under the proxy every poll is metered.
useRefresh(() => Promise.all([fetchMessages(), fetchWhy()]), { auto: true })

watch([filterQueue, pageSize, filterGroup, page], (values, previous) => {
  if (restoringRoute.value) fetchMessages()
  // A new group is an operator asking again: reload re-probes the replay route.
  else if (values[2] !== previous[2]) reload()
  else if (values[0] !== previous[0] || values[1] !== previous[1]) {
    page.value = 1
    fetchMessages()
  }
  if (values[0] !== previous[0]) fetchWhy()
})

// In setup, not onMounted: the first paint must be the loading state, not the
// "dead letter queue is empty" state we have not asked about yet.
refreshQueues()
fetchMessages()
fetchWhy()
</script>

<style scoped>
.dlq { display: flex; flex-direction: column; gap: 16px; }
.dlq > .page-tools, .dlq > .view-banner { margin-bottom: 0; }
/* Group names are long and dotted: the picker gets room. */
.dlq-group :deep(.ac) { width: 200px; }
/* The tools row's field drops the input's box; the picker keeps room for its
   chevron and clear button, and the whole width. */
.dlq-group :deep(.ac-input) { width: 100%; padding-right: 28px; }

/* --- Why they failed: one row per kind of error, the text in full ---------- */
.dlq-why { list-style: none; margin: 0; padding: 4px 0; }
.dlq-why li + li { border-top: 1px solid var(--bd-soft); }
.dlq-why-row {
  display: grid; grid-template-columns: 44px minmax(0, 1fr) auto; align-items: start; gap: 14px;
  width: 100%; padding: 10px 16px; border: 0; background: none; text-align: left; font: inherit; cursor: pointer;
  transition: background-color .12s var(--ease);
}
.dlq-why-row:hover:not(:disabled) { background: var(--ink-3); }
.dlq-why-row.on, .dlq-why-row.on:hover { background: var(--selected); }
.dlq-why-row:disabled { cursor: default; }
.dlq-why-row:focus-visible { outline-offset: -2px; }
.dlq-why-share { font-size: 13px; font-weight: 600; color: var(--text-hi); text-align: right; line-height: 20px; }
.dlq-why-body { min-width: 0; display: flex; flex-direction: column; gap: 6px; }
.dlq-why-text {
  font-family: var(--font-mono); font-size: 12px; line-height: 20px; color: var(--text-hi);
  white-space: pre-wrap; overflow-wrap: anywhere;
}
.dlq-why-bar { display: block; height: 3px; border-radius: 2px; background: var(--ink-4); overflow: hidden; }
.dlq-why-bar > span { display: block; height: 100%; background: var(--text-low); border-radius: 2px; }
.dlq-why-row.on .dlq-why-bar > span { background: var(--text-hi); }
.dlq-why-n { font-size: 12px; color: var(--text-low); line-height: 20px; white-space: nowrap; }
.dlq-why-more { display: flex; padding: 4px 16px 12px 74px; }
.dlq-link {
  padding: 0; border: none; background: none; cursor: pointer;
  font-size: 12px; color: var(--text-low); transition: color .12s var(--ease);
}
.dlq-link:hover { color: var(--text-hi); }

/* A narrowed table says so in its header; the error can be a sentence. */
.dlq-filter-chip {
  display: inline-flex; align-items: center; gap: 6px; max-width: min(340px, 100%); min-width: 0;
  padding: 1px 6px 1px 8px; border: 1px solid var(--bd-hi); border-radius: var(--r-chip);
  background: transparent; color: var(--text-mid); font-size: 12px; cursor: pointer;
}
.dlq-filter-chip:hover { color: var(--text-hi); }
.dlq-filter-chip-text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* --- The table ------------------------------------------------------------- */
.dlq-scroll { overflow-x: auto; }
.dlq-table { min-width: 760px; }
.dlq-table th, .dlq-table td { white-space: nowrap; }
.dlq-table td { height: 40px; font-size: 13px; }
.dlq-table .r { text-align: right; }
.dlq-row { cursor: pointer; }
.dlq-row:focus-visible { outline-offset: -2px; }
.dlq-row.dlq-row-on td { background: var(--selected); }
/* Long ids, names and errors end in an ellipsis; the whole is the tooltip. */
.dlq-cut { display: inline-block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: bottom; }
.dlq-txn { font-family: var(--font-mono); font-size: 12px; color: var(--text-mid); white-space: nowrap; }
.dlq-queue { max-width: 200px; font-weight: 500; color: var(--text-hi); }
.dlq-group-name { max-width: 180px; color: var(--text-mid); }
/* The error is the column the page exists for: strongest ink, no hue. */
.dlq-err-cell { max-width: 340px; font-family: var(--font-mono); font-size: 12px; color: var(--text-hi); }
.dlq-num { font-family: var(--font-mono); font-size: 12px; font-variant-numeric: tabular-nums; }
.dlq-when { font-size: 12px; color: var(--text-mid); font-variant-numeric: tabular-nums; }
.dlq-actions { width: 1%; }
.dlq-row-btn { padding: 3px 8px; font-size: 12px; }
.dlq-row-btn + .dlq-row-btn { margin-left: 4px; }
/* A row's Replay and Purge appear on the row under the pointer (or with
   focus inside it), not on every row at once. Touch screens keep them. */
@media (hover: hover) {
  .dlq-row .dlq-row-btn { opacity: 0; transition: opacity .12s var(--ease); }
  .dlq-row:hover .dlq-row-btn, .dlq-row:focus-within .dlq-row-btn, .dlq-row.dlq-row-on .dlq-row-btn { opacity: 1; }
}
.dlq-row-err { margin-top: 4px; max-width: 260px; margin-left: auto; white-space: normal; text-align: right; font-size: 12px; color: var(--ember-400); }

/* --- Drawer and modals ------------------------------------------------------ */
.dlq-quiet { margin: 0 0 8px; font-size: 12px; color: var(--text-low); }
.dlq-form { display: flex; flex-direction: column; gap: 8px; }
.dlq-form .panel-err { margin: 0 0 4px; }
.dlq-lead { margin: 0; font-size: 13px; color: var(--text-hi); overflow-wrap: anywhere; }
.dlq-lead strong { font-weight: 600; }
.dlq-help { margin: 0; font-size: 12px; color: var(--text-low); }
.dlq-purge-id { max-width: 240px; }

/* The replay confirm opens from the drawer (z 51) as well as from a row, so it
   sits above the drawer and under the Autocomplete's menu (60). */
.dlq-replay-over { z-index: 55; }
/* Wider than the 480px shell: a dotted queue, a partition and a dlq:<uuid>
   transaction id are all read here. */
.dlq-replay-card { max-width: 560px; }
.dlq-replay-body { display: grid; gap: 16px; }
.dlq-replay-lead { margin: 0; font-size: 13px; line-height: 1.5; color: var(--text-mid); }
.dlq-replay-lead strong { color: var(--text-hi); font-weight: 600; overflow-wrap: anywhere; }
.dlq-replay-sep { padding: 0 2px; color: var(--text-low); }
.dlq-replay-facts {
  margin: 0; padding: 8px 12px 8px 28px; display: grid; gap: 4px;
  font-size: 12px; line-height: 1.5; color: var(--text-mid);
  border: 1px solid var(--bd); border-radius: var(--r-control); background: var(--ink-3);
}
.dlq-replay-facts strong { color: var(--text-hi); font-weight: 500; overflow-wrap: anywhere; }
.dlq-toggle {
  display: flex; align-items: center; gap: 8px;
  padding: 0; border: none; background: none; cursor: pointer;
  font-size: 12px; font-weight: 500; color: var(--text-mid);
}
.dlq-toggle:hover { color: var(--text-hi); }
.dlq-toggle-chev { transition: transform .15s var(--ease); }
.dlq-toggle-open { transform: rotate(90deg); }
.dlq-toggle-badge { font-weight: 400; }
.dlq-replay-advanced { display: grid; gap: 16px; }
.dlq-replay-field { display: grid; gap: 6px; }
.dlq-field-label { font-size: 12px; color: var(--text-mid); }
.dlq-invalid { color: var(--ember-400); font-size: 12px; line-height: 1.45; }
.dlq-help .font-mono { color: var(--text-mid); overflow-wrap: anywhere; }

/* The four outcomes of a replay: neutral when it worked or wrote nothing,
   attention when it is half-known, failure when it did not happen. */
.dlq-verdict { padding: 12px; border: 1px solid var(--bd); border-radius: var(--r-card); font-size: 13px; line-height: 1.45; color: var(--text-hi); }
.dlq-verdict strong { display: block; font-weight: 600; margin-bottom: 4px; }
.dlq-verdict p { margin: 0; font-size: 12px; color: var(--text-mid); }
.dlq-verdict-warning { border-color: var(--warn-bd); background: var(--warn-glow); color: var(--warn-400); }
.dlq-verdict-error { border-color: var(--ember-bd); background: var(--ember-glow); color: var(--ember-400); }
.dlq-verdict-link { margin-top: 12px; max-width: 100%; overflow-wrap: anywhere; }

@media (max-width: 640px) {
  .dlq-group { flex: 1 1 100%; }
  .dlq-group :deep(.ac) { flex: 1; width: auto; }
}
.dlq-row-purge { color: var(--text-mid); }
.dlq-row-purge:hover:not(:disabled) { color: var(--ember-400); background: var(--ember-glow); }
</style>
