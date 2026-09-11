<template>
  <div class="view-container">

    <!-- Scope. These are the acting tenant's dead-lettered messages on the
         acting cluster, not the cell total. Built from identity, not from the
         fetch, so it states the scope while the list is loading, empty or
         failed — which is what the old tooltip was compensating for. -->
    <div class="scope-strip">
      <span class="chip chip-mute">tenant scope</span>
      <span class="scope-text">
        <strong>{{ actingTenantSlug || 'no tenant' }}</strong>
        <span class="scope-sep">/</span>{{ actingClusterSlug || 'no cluster' }}
        <span class="scope-sep">·</span>cell {{ actingCellSlug || 'unknown' }}
      </span>
      <span class="scope-fill"></span>
    </div>

    <!-- The list failed: say so instead of drawing an empty, healthy-looking page. -->
    <div v-if="error" class="status-banner banner-bad view-banner">
      <span>
        <strong>Could not load the dead-letter queue</strong> · {{ describeApiError(error) }}<template v-if="messages.length">
          · showing the last rows that loaded{{ lastUpdatedText ? ` (${lastUpdatedText})` : '' }}</template>
      </span>
    </div>

    <!-- A broker capability, not a data fact — and it belongs above the table,
         not inside the pager it disables. -->
    <!-- Informational: it describes what this broker CAN do, which is not a
         condition of the system and never becomes untrue by itself. -->
    <div v-if="!serverPaginates" class="status-banner banner-info view-banner">
      <span>This broker returns the whole dead-letter queue at once — paging is disabled</span>
    </div>

    <!-- Filters. Applied server-side (the endpoint takes queue + consumerGroup),
         so they select from the whole DLQ, not from the loaded page. -->
    <div class="card filters">
      <div class="card-body filter-rows">
        <div class="filter-row">
          <!-- Autocomplete, not a select: a cell carries hundreds of queues and
               the names are long and dotted, so scanning one flat list is the
               slow way to reach `smartchat.agent.document-to-process`. Free
               entry stays open because the queue list and the DLQ come from two
               different endpoints — a queue that only exists in the DLQ must
               still be filterable. -->
          <div class="filter-field-col filter-field-wide">
            <label class="label-xs" for="dlq-queue-filter">Queue</label>
            <Autocomplete
              id="dlq-queue-filter"
              v-model="filterQueue"
              :options="queueOptions"
              :loading="queuesLoading"
              label="Queue"
              placeholder="All queues"
              allow-custom
            />
            <span v-if="queuesUnavailable" class="filter-hint">
              Queue list unavailable - type a name to filter
            </span>
            <span v-else-if="unlistedQueue" class="filter-hint">
              Not in this cluster's queue list - filtering by name anyway
            </span>
          </div>
          <div class="filter-field-col">
            <label class="label-xs">Consumer group</label>
            <input
              v-model="filterGroup"
              class="input"
              list="dlq-group-options"
              placeholder="All groups"
              @change="reload"
              @keyup.enter="reload"
            />
            <datalist id="dlq-group-options">
              <option v-for="g in groupSuggestions" :key="g" :value="g" />
            </datalist>
          </div>
          <div class="filter-field-col">
            <label class="label-xs">Page size</label>
            <select v-model.number="pageSize" class="input">
              <option :value="50">50</option>
              <option :value="100">100</option>
              <option :value="200">200</option>
            </select>
          </div>
          <div v-if="canAdmin" class="filter-field-col dlq-bulk-action">
            <label class="label-xs">Bulk cleanup</label>
            <button
              class="btn btn-danger"
              :disabled="!filterQueue.trim() || bulkPurging"
              :title="filterQueue.trim() ? bulkPurgeButtonTitle : 'Choose a queue before bulk purging'"
              @click="openBulkPurge"
            >
              Purge by criteria
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Summary. What an operator opens this page for is WHICH failure is
         dominating, so that is the only summary kept: a count of rows on the
         page and a partition tally over the same rows both dressed a page-sized
         fact as a queue-sized one (the broker reports no DLQ total, and the
         proxy blocks /api/v1/status for tenants).
         Rendered only with rows to describe — a breakdown of nothing is noise,
         and the table below owns the loading and failure states. -->
    <div v-if="pageMessages.length" class="card" style="margin-bottom:16px;">
      <div class="card-header">
        <h3>Failure breakdown</h3>
        <span class="card-sub">
          {{ errorGroups.length }} distinct {{ errorGroups.length === 1 ? 'error' : 'errors' }}
          across the {{ formatNumber(pageMessages.length) }} messages on this page
        </span>
        <span class="muted">{{ stamp(listPanel) }}</span>
      </div>
      <div class="card-body dlq-breakdown">
        <!-- The shape of the failure in one 6px strip: whether this page is one
             recurring fault or a long tail of unrelated ones. -->
        <div class="dlq-dist">
          <button
            v-for="seg in distribution"
            :key="seg.error"
            type="button"
            class="dlq-dist-seg"
            :class="{ 'dlq-dist-seg-on': errorFilter === seg.error }"
            :style="{ width: seg.share * 100 + '%', opacity: segmentOpacity(seg) }"
            :title="`${seg.count} · ${seg.error}`"
            :aria-label="`${seg.count} messages: ${seg.error}`"
            @click="toggleErrorFilter(seg.error)"
          />
        </div>

        <!-- Two columns, so six errors cost three rows of height. Each row is a
             filter for the table below, not a label. No percentage column: the
             default page size is 100, where a share and a count are the same
             two digits — the strip above already carries the proportion. -->
        <div class="dlq-err-grid">
          <button
            v-for="entry in visibleErrorGroups"
            :key="entry.error"
            type="button"
            class="dlq-err"
            :class="{ 'dlq-err-on': errorFilter === entry.error }"
            :title="entry.error"
            :aria-pressed="errorFilter === entry.error"
            @click="toggleErrorFilter(entry.error)"
          >
            <span class="dlq-err-count font-mono tabular-nums">{{ entry.count }}</span>
            <span class="dlq-err-text">{{ entry.error }}</span>
          </button>
        </div>

        <div v-if="errorGroups.length > COLLAPSED_ERRORS" class="dlq-err-more">
          <button type="button" class="dlq-link" @click="showAllErrors = !showAllErrors">
            {{ showAllErrors ? `Show top ${COLLAPSED_ERRORS}` : `Show all ${errorGroups.length}` }}
          </button>
        </div>
      </div>
    </div>

    <!-- Messages table -->
    <div class="card" style="margin-bottom:16px;">
      <div class="card-header">
        <h3>Dead-lettered messages</h3>
        <span class="chip chip-mute">{{ formatNumber(pageMessages.length) }} loaded</span>
        <span class="chip chip-mute">page <span class="font-mono tabular-nums">{{ page }}</span></span>
        <!-- A narrowed table must say so where the row count is read, not only
             up in the breakdown that narrowed it. -->
        <button v-if="errorFilter" type="button" class="chip chip-mute dlq-filter-chip" @click="errorFilter = null">
          <span class="dlq-filter-chip-text">{{ formatNumber(messages.length) }} shown · {{ errorFilter }}</span>
          <span aria-hidden="true">×</span>
          <span class="sr-only">Clear the error filter</span>
        </button>
        <span class="muted">{{ stamp(listPanel) }}</span>
      </div>

      <div style="overflow-x:auto;">
        <table class="t">
          <thead>
            <tr>
              <th>Message</th>
              <th>Queue</th>
              <th>Consumer</th>
              <th>Error</th>
              <th>Retries</th>
              <th>Failed</th>
              <th v-if="canAdmin" style="text-align:right;">Actions</th>
            </tr>
          </thead>
          <tbody>
            <!-- First paint only: a refresh or a page turn leaves the rows that
                 already loaded on screen. -->
            <template v-if="firstLoad">
              <tr v-for="i in 8" :key="i">
                <td><div class="skeleton" style="height:16px; width:112px;" /></td>
                <td><div class="skeleton" style="height:16px; width:88px;" /></td>
                <td><div class="skeleton" style="height:16px; width:104px;" /></td>
                <td><div class="skeleton" style="height:16px; width:168px;" /></td>
                <td><div class="skeleton" style="height:16px; width:32px;" /></td>
                <td><div class="skeleton" style="height:16px; width:72px;" /></td>
                <td v-if="canAdmin"><div class="skeleton" style="height:16px; width:56px; margin-left:auto;" /></td>
              </tr>
            </template>

            <template v-else-if="messages.length">
              <!--
                EVERY row here is a failure — that is what the page is — so
                nothing in a row is painted as one. A red dot, red error text
                and an amber selection wash on all of them carried no
                information beyond the table's own title, and left the page
                with no way to say that something is failing RIGHT NOW. The
                error text instead gets the page's strongest ink, because it
                is the column the reader came for.
              -->
              <tr
                v-for="msg in messages"
                :key="rowKey(msg)"
                class="dlq-row"
                :class="{ 'dlq-row-on': selectedKey && rowKey(msg) === selectedKey }"
                @click="selectMessage(msg)"
              >
                <td>
                  <div style="display:flex; align-items:center; gap:6px;">
                    <span class="dlq-row-dot" />
                    <span class="font-mono" style="font-size:12px;">{{ (msg.transactionId || msg.id || '-').slice(0, 14) }}…</span>
                  </div>
                </td>
                <td style="font-weight:500;">{{ msg.queue || '-' }}</td>
                <td class="font-mono" style="font-size:12px; color:var(--text-mid);">{{ msg.consumerGroup || '-' }}</td>
                <td>
                  <span class="font-mono dlq-err-cell">{{ truncateError(msg.errorMessage) }}</span>
                </td>
                <td class="font-mono" style="font-size:12px;">{{ msg.retryCount ?? '—' }}</td>
                <td class="font-mono" style="font-size:12px; color:var(--text-mid);">{{ formatRelativeTime(msg.failedAt) }}</td>
                <td v-if="canAdmin" style="text-align:right; white-space:nowrap;" @click.stop>
                  <!-- Replay before Purge: the recoverable action reads first,
                       and the destructive one keeps the far-right position it
                       has always had. A row whose listing carries no id cannot
                       be addressed by the move route at all, so the button says
                       why instead of disappearing. -->
                  <button
                    class="btn btn-ghost dlq-row-btn"
                    :disabled="!msg.id || isReplaying(msg) || replayUnavailable"
                    :title="replayUnavailable
                      ? 'This cell does not serve the replay route — the broker predates it, or the proxy in front does not classify it'
                      : (msg.id
                        ? `Replay this message onto ${msg.queue || 'its queue'}`
                        : 'This broker’s dead-letter list carries no row id, so a replay cannot address this row')"
                    @click="openReplay(msg)"
                  >
                    {{ isReplaying(msg) ? '…' : 'Replay' }}
                  </button>
                  <button class="btn btn-danger" style="padding:4px 10px; font-size:11px;" @click="purge(msg)" :disabled="isDeleting(msg)">
                    {{ isDeleting(msg) ? '…' : 'Purge' }}
                  </button>
                  <!-- A purge the broker refused must stay on screen: the row is
                       still here, and so is the reason. -->
                  <div v-if="rowError(msg)" style="font-size:11px; color:var(--ember-400); margin-top:4px; max-width:260px; white-space:normal;">
                    {{ rowError(msg) }}
                  </div>
                </td>
              </tr>
            </template>

            <!-- Never an idle empty state on a failure: that is a load error
                 wearing "no dead letters" as a disguise. -->
            <tr v-else-if="error">
              <td :colspan="canAdmin ? 7 : 6">
                <div class="empty-state empty-state-failed">
                  <h3>{{ describeApiError(error) }}</h3>
                  <p>Nothing loaded — this is a failure, not an empty dead-letter queue.</p>
                </div>
              </td>
            </tr>

            <tr v-else>
              <td :colspan="canAdmin ? 7 : 6">
                <div class="empty-state">
                  <svg class="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M5 7h14l-1.2 11.2a2 2 0 01-2 1.8H8.2a2 2 0 01-2-1.8L5 7Z" />
                    <path stroke-linecap="round" stroke-linejoin="round" d="M9 4h6v3H9z" />
                  </svg>
                  <h3>{{ page > 1 ? 'No more dead-lettered messages' : 'Dead letter queue is empty' }}</h3>
                  <p>{{ hasFilters ? 'No failed messages match these filters.' : 'No failed messages to review.' }}</p>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Not drawn under an empty first page, under a failure, or on a broker
           that ignores the limit: Previous/Next there offer travel that goes
           nowhere. -->
      <div v-if="serverPaginates && (pageMessages.length || page > 1)" class="pager">
        <span class="pager-count">Page <span class="font-mono tabular-nums">{{ page }}</span></span>
        <div class="pager-nav">
          <button class="btn btn-ghost" :disabled="page === 1" @click="prevPage">Previous</button>
          <button class="btn btn-ghost" :disabled="!canPageForward" @click="nextPage">Next</button>
        </div>
      </div>
    </div>

    <!-- Detail drawer (teleported to body to avoid transform issues).
         Backdrop before the panel, so DOM order matches paint order. -->
    <Teleport to="body">
      <div v-if="bulkTarget" class="modal-backdrop" @click.self="closeBulkPurge">
        <div class="card modal-card">
          <div class="card-header"><h3>Purge matching DLQ records</h3></div>
          <div class="card-body">
            <div v-if="bulkPurgeError" class="panel-err">{{ bulkPurgeError }}</div>
            <p>
              This will permanently purge every dead-letter record for queue
              <strong class="font-mono">{{ bulkTarget.queue }}</strong><template v-if="bulkTarget.consumerGroup">
                and consumer group <strong class="font-mono">{{ bulkTarget.consumerGroup }}</strong></template>.
            </p>
            <p class="dlq-bulk-note">
              The error breakdown filter is page-only and is not part of this cleanup.
              This action cannot be undone.
            </p>
          </div>
          <div class="modal-foot">
            <button class="btn btn-ghost" :disabled="bulkPurging" @click="closeBulkPurge">Cancel</button>
            <button class="btn btn-danger" :disabled="bulkPurging" @click="purgeByCriteria">
              {{ bulkPurging ? 'Purging…' : 'Purge all matching records' }}
            </button>
          </div>
        </div>
      </div>
    </Teleport>

    <!-- Replay. A confirm rather than a one-click action because a replay is a
         WRITE: it appends a copy to a partition, it removes the dead-letter
         record, and both are irreversible. So the modal names the target AND
         the irreversible part before the click, the way the maintenance toggle
         taught this dashboard to. -->
    <Teleport to="body">
      <div v-if="replayRow" class="modal-backdrop dlq-replay-over" @click.self="closeReplay">
        <form class="card modal-card dlq-replay-card" @submit.prevent="submitReplay">
          <div class="card-header">
            <h3>{{ replayTarget.moved ? 'Move and replay' : 'Replay dead-letter message' }}</h3>
            <span class="card-sub font-mono">{{ replayRow.consumerGroup || 'no consumer group' }}</span>
          </div>

          <div class="card-body dlq-replay-body">
            <!-- The verdict, and the only thing on screen allowed to call a
                 replay a success. It does not replace the form: a refusal or a
                 broker fault leaves the destination fields exactly as they were
                 so they can be corrected and sent again. A verdict that names a
                 FIELD is not drawn here at all — it belongs under that field,
                 which is where the correction is made. -->
            <div
              v-if="replayResult && !replayResult.field"
              class="dlq-verdict"
              :class="`dlq-verdict-${replayResult.kind}`"
            >
              <strong>{{ replayResult.title }}</strong>
              <p>{{ replayResult.detail }}</p>
              <!-- Only ever offered for the two answers that name a destination
                   the message is actually in. -->
              <router-link
                v-if="replayResult.target"
                class="btn btn-ghost dlq-verdict-link"
                :to="{ path: '/messages', query: {
                  queue: replayResult.target.queue,
                  partition: replayResult.target.partition,
                } }"
              >
                Open {{ replayResult.target.queue }} in Messages
              </router-link>
            </div>

            <!-- Gone once the dead-letter row is verified gone: there is nothing
                 left to replay, and a live form under a finished verdict invites
                 a second attempt at a row that no longer exists. -->
            <template v-if="!replayDone">
              <p class="dlq-replay-lead">
                This appends a new copy at the tail of
                <strong class="font-mono">{{ replayTarget.queue }}</strong><span class="dlq-replay-sep">/</span><strong class="font-mono">{{ replayTarget.partition }}</strong>
                with transaction id <strong class="font-mono">{{ replayTarget.transactionId }}</strong>.
                <!-- Which of the two the request actually carries is not a
                     detail here: a destination is admitted as one pair, so the
                     button either names both halves or names neither, and the
                     sentence says which of the two it is about to do. -->
                <template v-if="replayTarget.namesDestination">
                  The request carries that destination explicitly, queue and partition together:
                  broker-direct, an omitted half falls back to this row's own value without
                  saying so, and a cloud proxy refuses a half-named destination outright, so the
                  whole pair is the one form that means the same thing in both.
                </template>
                <template v-else>
                  No destination is sent: the broker replays onto the queue and partition the
                  message failed on, which are the two named above.
                </template>
              </p>

              <ul class="dlq-replay-facts">
                <li>
                  The dead-letter record is removed in the same transaction as the copy — one move,
                  not a push followed by a cleanup that can fail on its own. If nothing is written
                  (the destination already carries this transaction id), nothing is removed either.
                </li>
                <li>
                  Records of other consumer groups for the same message are untouched: this replays
                  <strong class="font-mono">{{ replayRow.consumerGroup || 'this group' }}</strong>’s row and only that one.
                </li>
                <li>
                  Replay appends — it does not restore the message’s position in the partition, and its
                  age clock restarts at the destination.
                </li>
                <li v-if="replayTarget.moved">
                  This is a move, not a replay in place: the message failed on
                  <strong class="font-mono">{{ replayRow.queue }}/{{ replayRow.partition }}</strong> and a destination this
                  cluster does not carry yet is created by the replay, with the default options.
                </li>
              </ul>

              <!-- The destination override the route has carried from day one.
                   Behind a fold because replaying where the message failed is
                   the answer nearly every time, and a target queue left over
                   from a previous row is how a message lands in the wrong
                   place. -->
              <button type="button" class="dlq-toggle" @click="showReplayAdvanced = !showReplayAdvanced">
                <span class="dlq-toggle-chev" :class="{ 'dlq-toggle-open': showReplayAdvanced }" aria-hidden="true">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path stroke-linecap="round" stroke-linejoin="round" d="M9 5l7 7-7 7" />
                  </svg>
                </span>
                Advanced — replay somewhere else
                <span v-if="replayTarget.moved" class="chip chip-warn dlq-toggle-badge">destination changed</span>
              </button>

              <div v-if="showReplayAdvanced" class="dlq-replay-advanced">
                <label class="dlq-replay-field">
                  <span class="label-xs">Target queue</span>
                  <Autocomplete
                    v-model="replayForm.queue"
                    :options="queueOptions"
                    :loading="queuesLoading"
                    label="Queue"
                    :placeholder="replayRow.queue || 'Where it failed'"
                    allow-custom
                  />
                  <span v-if="replayResult?.field === 'queue'" class="dlq-invalid">{{ replayResult.detail }}</span>
                  <span v-else class="dlq-help">
                    Blank replays onto <span class="font-mono">{{ replayRow.queue || 'the queue it failed on' }}</span>.
                    Pick a name, or press Enter to accept one this cluster does not carry yet — the picker
                    drops text it was never told to apply, and the sentence above always names the
                    destination this button will write to.
                  </span>
                </label>

                <label class="dlq-replay-field">
                  <span class="label-xs">Target partition</span>
                  <input
                    v-model="replayForm.partition"
                    class="input font-mono"
                    autocomplete="off"
                    spellcheck="false"
                    :placeholder="replayRow.partition || 'Where it failed'"
                  />
                  <span v-if="replayResult?.field === 'partition'" class="dlq-invalid">{{ replayResult.detail }}</span>
                  <span v-else class="dlq-help">
                    Blank keeps <span class="font-mono">{{ replayRow.partition || 'the partition it failed on' }}</span>.
                    Fill either field and the request names the whole pair. Broker-direct, a missing half
                    would quietly fall back to this row's own value; a cloud proxy refuses a half-named
                    destination instead, since it cannot see the half it was not given.
                  </span>
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
              <!-- Disabled in flight, and the row stays in the table behind it
                   until the verdict arrives: the broker's answer is what removes
                   a row here, never the click that asked for it. -->
              <button type="submit" class="btn btn-primary" :disabled="replaying || !replayTarget.id">
                {{ replaying ? 'Replaying…' : (replayTarget.moved ? 'Move and replay' : 'Replay message') }}
              </button>
            </template>
          </div>
        </form>
      </div>
    </Teleport>

    <DetailDrawer
      :open="Boolean(selectedMsg)"
      title="DLQ Message Detail"
      :subtitle="selectedMsg?.transactionId || selectedMsg?.id || ''"
      wide
      split
      @close="closeDetail"
    >
      <template #actions>
        <button
          class="btn btn-ghost"
          title="Copy the failure, routing details, timestamps, and payload as Markdown"
          @click="copyMarkdown"
        >
          {{ markdownCopied ? 'Markdown copied!' : 'Copy as Markdown' }}
        </button>
      </template>

      <template v-if="selectedMsg">
        <div class="detail-status-row">
          <span class="chip chip-bad">dead_letter</span>
          <!-- A retry count is a count: how hard the broker tried before it
               gave up. The `dead_letter` chip beside it carries the verdict. -->
          <span v-if="selectedMsg.retryCount" class="chip chip-mute">
            {{ selectedMsg.retryCount }} retries
          </span>
        </div>

        <div class="detail-fields">
          <DetailField label="Queue" :value="selectedMsg.queue" tone="high" />
          <DetailField label="Partition" :value="selectedMsg.partition" mono copyable />
          <DetailField label="Partition ID" :value="selectedMsg.partitionId" mono copyable />
          <DetailField label="Transaction ID" :value="selectedMsg.transactionId" mono copyable />
          <DetailField label="Consumer group" :value="selectedMsg.consumerGroup" mono tone="accent" />

          <!-- The log engine cannot recover the enqueue time from an opaque
               blob, so it echoes failed_at as createdAt. Showing the same
               instant twice under two labels invents a fact. -->
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
            <span v-if="!hasDistinctCreatedAt(selectedMsg)" class="detail-note">
              Enqueue time is not recorded for this entry.
            </span>
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
          tone="danger"
        />

        <!-- Actions. Replay re-pushes this snapshot on the broker's move
             primitive — lock, push, delete, one transaction — so the button can
             state what happened to the row instead of guessing; purge is the
             end of the line for a message nobody will process. -->
        <div v-if="canAdmin" class="detail-actions">
          <button
            class="btn btn-ghost"
            style="width:100%; justify-content:center;"
            :disabled="!selectedMsg.id || isReplaying(selectedMsg) || replayUnavailable"
            :title="replayUnavailable
              ? 'This cell does not serve the replay route — the broker predates it, or the proxy in front does not classify it'
              : (selectedMsg.id
                ? null
                : 'This broker’s dead-letter list carries no row id, so a replay cannot address this row')"
            @click="openReplay(selectedMsg)"
          >
            {{ isReplaying(selectedMsg) ? 'Replaying…' : 'Replay message' }}
          </button>
          <button
            class="btn btn-danger"
            style="width:100%; justify-content:center;"
            :disabled="isDeleting(selectedMsg)"
            @click="purge(selectedMsg)"
          >
            {{ isDeleting(selectedMsg) ? 'Purging…' : 'Purge message' }}
          </button>
          <p v-if="rowError(selectedMsg)" style="font-size:12px; color:var(--ember-400);">
            {{ rowError(selectedMsg) }}
          </p>
        </div>
        <p v-else class="detail-actions detail-note">
          Replaying and purging need the admin role on this cluster.
        </p>
      </template>

      <template #secondary>
        <div v-if="selectedMsg?.data !== undefined">
          <div class="detail-section-header">
            <label class="label-xs">Payload</label>
            <button class="btn btn-ghost detail-copy-button" @click="copyPayload">
              {{ copied ? 'Copied!' : (encryptedPayload ? 'Copy envelope' : 'Copy') }}
            </button>
          </div>
          <!-- The DLQ read path does no decryption: what follows is the stored
               envelope, not the message. Saying "payload" over ciphertext is
               how a debugger loses an hour. -->
          <!-- A fact about the payload, not a fault: information. -->
          <div v-if="encryptedPayload" class="status-banner banner-info view-banner">
            <span>
              <strong>Encrypted envelope</strong> · this queue encrypts payloads and the DLQ endpoint
              returns them as stored. This is ciphertext, not the message body.
            </span>
          </div>
          <JsonViewer :value="selectedMsg.data" />
        </div>
      </template>
    </DetailDrawer>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { dlq, queues as queuesApi, describeApiError } from '@/api'
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
import DetailDrawer from '@/components/DetailDrawer.vue'
import DetailField from '@/components/DetailField.vue'
import JsonViewer from '@/components/JsonViewer.vue'

const { can, epoch, actingTenantSlug, actingClusterSlug, actingCellSlug } = useIdentity()
const { notifySuccess, notifyInfo, notifyWarn, notifyError } = useToast()

/** Error groups shown before the breakdown has to be expanded. Two columns, so
    an even number keeps the grid square. */
const COLLAPSED_ERRORS = 6
/** Bars in the distribution strip; everything past this merges into one tail. */
const DIST_SEGMENTS = 8
const TAIL_SEGMENT = '__tail__'
/** Errors the broker sent with no text: still a group, and still says so. */
const NO_ERROR_TEXT = '(no error text recorded)'

const errorKey = (msg) => msg.errorMessage || NO_ERROR_TEXT

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
const pageSize = ref(100)
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
  error: queuesError,
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
// The picker suggests from /resources/queues, but the filter is applied by the
// DLQ endpoint. When the two disagree, say which one is being trusted instead
// of leaving a name in the box that looks unrecognised.
const queuesUnavailable = computed(() => Boolean(queuesError.value) && queueOptions.value.length === 0)
const unlistedQueue = computed(
  () => Boolean(filterQueue.value) && queueOptions.value.length > 0 && !queueOptions.value.includes(filterQueue.value)
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
// Failure breakdown — the page's only summary, and the table's filter
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

const visibleErrorGroups = computed(
  () => (showAllErrors.value ? errorGroups.value : errorGroups.value.slice(0, COLLAPSED_ERRORS))
)

/** Segments of the distribution strip: the top groups, then one bar for the
    tail, so the widths always add up to the page. */
const distribution = computed(() => {
  const head = errorGroups.value.slice(0, DIST_SEGMENTS)
  const segments = head.map((entry, i) => ({
    ...entry,
    // The palette carries one "bad" hue, so rank is stepped in opacity.
    opacity: Math.max(0.32, 1 - i * 0.11),
  }))
  const tail = errorGroups.value.slice(DIST_SEGMENTS)
  if (tail.length) {
    const count = tail.reduce((sum, e) => sum + e.count, 0)
    segments.push({
      error: TAIL_SEGMENT,
      count,
      share: count / (pageMessages.value.length || 1),
      opacity: 0.22,
    })
  }
  return segments
})

/** A selection mutes every other bar, so the strip reads as "this slice of the
    page" instead of staying a full-width chart of something else. */
const segmentOpacity = (seg) => {
  if (!errorFilter.value) return seg.opacity
  return errorFilter.value === seg.error ? 1 : 0.14
}

/** Toggle, not set: a second click on the active error clears the filter. */
const toggleErrorFilter = (error) => {
  if (error === TAIL_SEGMENT) return
  errorFilter.value = errorFilter.value === error ? null : error
}

const groupSuggestions = computed(
  () => [...new Set(pageMessages.value.map(m => m.consumerGroup).filter(Boolean))].sort()
)

const hasFilters = computed(() => Boolean(filterQueue.value || filterGroup.value || errorFilter.value))
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

const truncateError = (err) => {
  if (!err || err === '-') return '-'
  return err.length > 50 ? err.slice(0, 50) + '…' : err
}

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

const purge = async (msg) => {
  if (!canAdmin.value) return
  const key = msgKey(msg)
  if (!confirm(`Purge message ${String(key).slice(0, 16)}…?\n\nThis cannot be undone.`)) return
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
useRefresh(fetchMessages, { auto: true })

watch([filterQueue, pageSize], reload)

// In setup, not onMounted: the first paint must be the loading state, not the
// "dead letter queue is empty" state we have not asked about yet.
refreshQueues()
fetchMessages()
</script>

<style scoped>
/* Queue names here are dotted and long (`connect.newsletter.sendgrid`), so the
   picker gets more room than the 220px the shared filter column allows. */
.filter-field-wide { flex-basis: 240px; max-width: 300px; }
.dlq-bulk-action { flex: 0 0 auto; }
.dlq-bulk-note { margin-top: 12px; color: var(--text-low); font-size: 12px; }

/* --- Failure breakdown -----------------------------------------------------
   The page's summary, so it is sized like a strip and not like a panel: a 6px
   distribution bar over two columns of rows, roughly a third of the height the
   three stat tiles and the bottom histogram used to cost between them. */
.dlq-breakdown { display: flex; flex-direction: column; gap: 12px; }

.dlq-dist { display: flex; align-items: stretch; gap: 2px; height: 6px; }
/* The distribution of a failure list across its signatures: every segment is
   a failure, so the bar is drawn in ink and the SELECTED segment is the only
   one that stands out. Painting all of them red made the widest segment and
   the narrowest one equally urgent. */
.dlq-dist-seg {
  height: 100%; min-width: 3px; padding: 0; border: none; cursor: pointer;
  border-radius: 2px; background: var(--text-low);
  transition: background .12s var(--ease), transform .12s var(--ease);
}
.dlq-dist-seg:hover { background: var(--text-mid); transform: scaleY(1.5); }
.dlq-dist-seg-on { background: var(--text-hi); transform: scaleY(1.5); box-shadow: 0 0 0 1px var(--bd-hi); }

.dlq-err-grid {
  display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 1px 18px;
}
@media (max-width: 900px) { .dlq-err-grid { grid-template-columns: minmax(0, 1fr); } }

.dlq-err {
  display: flex; align-items: baseline; gap: 8px; width: 100%;
  padding: 3px 7px; border-radius: var(--r-chip);
  border: 1px solid transparent; background: none; cursor: pointer;
  text-align: left; transition: background .12s var(--ease), border-color .12s var(--ease);
}
.dlq-err:hover { background: var(--ink-3); }
/* Selected = selected, the app's own selected surface. It used to be a red
   wash, which read as "this signature is the dangerous one". */
.dlq-err-on { background: var(--ink-4); border-color: var(--bd-hi); }
.dlq-err-count {
  min-width: 34px; text-align: right; flex-shrink: 0;
  font-size: 13px; color: var(--text-hi);
}
.dlq-err-text {
  flex: 1; min-width: 0;
  font-family: 'JetBrains Mono', monospace; font-size: 11.5px; color: var(--text-hi);
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}

/* Row chrome. `cursor` and the selection wash used to be inline styles on the
   <tr>; the wash is now the neutral selected surface, and the leading dot is
   ink instead of a pulsing red one. */
.dlq-row { cursor: pointer; }
.dlq-row-on { background: var(--ink-4); }
.dlq-row-dot {
  width: 5px; height: 5px; border-radius: var(--r-pill);
  background: var(--text-faint); flex-shrink: 0;
}
/* The error is the column the page exists for: strongest ink, no hue. */
.dlq-err-cell { font-size: 12px; color: var(--text-hi); }

.dlq-err-more { display: flex; }
.dlq-link {
  padding: 0; border: none; background: none; cursor: pointer;
  font-size: 11px; color: var(--text-low); transition: color .12s var(--ease);
}
.dlq-link:hover { color: var(--text-hi); }

/* The active error can be a whole sentence: it truncates rather than pushing
   the freshness stamp off the header. */
.dlq-filter-chip { cursor: pointer; max-width: 340px; }
.dlq-filter-chip-text { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }

/* --- Replay ----------------------------------------------------------------
   Matched to the Purge button beside it — same padding, same type size — so the
   pair reads as one control group and not as a primary next to an afterthought.
   Ghost, because the destructive one is the only red on the row. */
.dlq-row-btn { padding: 4px 10px; font-size: 11px; margin-right: 6px; }

/* One of the two entry points is a button INSIDE the DLQ drawer, and the drawer
   is `z-index: 51` while the shared `.modal-backdrop` is 50 — the confirm would
   open behind the panel that raised it. 55 clears the drawer and still passes
   under the Autocomplete's teleported menu (60), which has to open inside this
   form. */
.dlq-replay-over { z-index: 55; }
/* Wider than the 480px shell: the destination is a dotted queue name plus a
   partition plus a `dlq:<uuid>` transaction id, and all three are read here. */
.dlq-replay-card { max-width: 560px; }

.dlq-replay-body { display: grid; gap: 14px; }
.dlq-replay-lead { margin: 0; font-size: 12.5px; line-height: 1.5; color: var(--text-mid); }
.dlq-replay-lead strong { color: var(--text-hi); font-weight: 600; overflow-wrap: anywhere; }
.dlq-replay-sep { padding: 0 2px; color: var(--text-low); }

/* The three (sometimes four) facts a replay cannot be undone without. A list,
   not a paragraph: each one is a separate consequence and they are checked
   individually before the button is pressed. */
.dlq-replay-facts {
  margin: 0; padding: 10px 12px 10px 26px;
  display: grid; gap: 6px;
  font-size: 11.5px; line-height: 1.45; color: var(--text-mid);
  border: 1px solid var(--bd); border-radius: var(--r-control);
  background: var(--ink-3);
}
.dlq-replay-facts strong { color: var(--text-hi); font-weight: 500; overflow-wrap: anywhere; }

.dlq-toggle {
  display: flex; align-items: center; gap: 8px;
  padding: 0; border: none; background: none; cursor: pointer;
  font-size: 12px; font-weight: 600; color: var(--text-mid);
}
.dlq-toggle:hover { color: var(--text-hi); }
.dlq-toggle-chev { display: inline-flex; width: 12px; height: 12px; transition: transform .15s var(--ease); }
.dlq-toggle-chev svg { width: 12px; height: 12px; }
.dlq-toggle-open { transform: rotate(90deg); }
.dlq-toggle-badge { font-weight: 500; }

.dlq-replay-advanced { display: grid; gap: 14px; }
.dlq-replay-field { display: grid; gap: 6px; }
.dlq-help { color: var(--text-low); font-size: 11.5px; line-height: 1.45; }
.dlq-help .font-mono { color: var(--text-mid); overflow-wrap: anywhere; }
.dlq-invalid { color: var(--ember-400); font-size: 11.5px; line-height: 1.45; }

/* The verdict block. `.panel-err` is single-toned and `.status-banner` has no
   success variant, so the four outcomes of a replay get one block with four
   tones, built from the same tokens so both schemes follow. Info is the ice
   hue: a duplicate wrote nothing, which is neither a success nor a warning. */
.dlq-verdict {
  padding: 10px 12px;
  border: 1px solid; border-radius: var(--r-card);
  font-size: 12.5px; line-height: 1.45;
}
.dlq-verdict strong { display: block; font-weight: 600; margin-bottom: 4px; }
.dlq-verdict p { margin: 0; color: var(--text-mid); }
.dlq-verdict-success { border-color: var(--ok-bd); background: var(--ok-glow); color: var(--ok-500); }
.dlq-verdict-info { border-color: var(--ice-bd); background: var(--ice-glow); color: var(--ice-400); }
.dlq-verdict-warning { border-color: var(--warn-bd); background: var(--warn-glow); color: var(--warn-400); }
.dlq-verdict-error { border-color: var(--ember-bd); background: var(--ember-glow); color: var(--ember-400); }
.dlq-verdict-link { margin-top: 10px; max-width: 100%; overflow-wrap: anywhere; }

</style>
