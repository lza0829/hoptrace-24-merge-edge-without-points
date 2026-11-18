<script setup>
import { computed, onMounted, ref } from 'vue'
import { api } from '../api.js'

const batches = ref([])
const selected = ref(null)
const proof = ref(null)
const busy = ref('')
const err = ref('')
const lastMode = ref('')

const committed = computed(() => proof.value?.status === 'committed')
const delta = computed(() => {
  if (!proof.value) return null
  const b = proof.value.projection_before || {}
  const a = proof.value.projection_after || {}
  return { before: b.status_proj ?? '—', after: a.status_proj ?? '—' }
})

async function loadBatches() {
  batches.value = await api.mergeBatches()
}

async function openBatch(id) {
  selected.value = id
  proof.value = await api.mergeBatch(id)
}

async function runFixture(mode) {
  busy.value = mode
  err.value = ''
  try {
    const result = await api.mergeFixture(mode)
    lastMode.value = mode
    proof.value = result
    selected.value = result.batch_id
    await loadBatches()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = ''
  }
}

onMounted(async () => {
  try {
    await loadBatches()
    if (batches.value.length) await openBatch(batches.value[0].batch_id)
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>

<template>
  <div class="page-card merge-page">
    <h1>并单</h1>
    <p class="lead">
      成员段正式点按 (event_ts, id) 并入主单正式时间线。同事务写点 + 写边 + 重算投影，任一步失败整笔回滚。
    </p>

    <div class="merge-actions">
      <button class="btn" :disabled="!!busy" @click="runFixture('success')">
        {{ busy === 'success' ? '合并中…' : '跑成功合并夹具' }}
      </button>
      <button class="btn btn-ghost" :disabled="!!busy" @click="runFixture('failure')">
        {{ busy === 'failure' ? '回滚中…' : '跑中途失败夹具（对照）' }}
      </button>
    </div>
    <p v-if="err" class="muted" style="margin-top:12px">{{ err }}</p>

    <div class="merge-grid">
      <aside class="batch-list">
        <h2>合并批次</h2>
        <button
          v-for="b in batches"
          :key="b.batch_id"
          class="batch-item"
          :class="{ active: selected === b.batch_id }"
          @click="openBatch(b.batch_id)"
        >
          <span class="bid">{{ b.batch_id }}</span>
          <span class="bmeta">{{ b.master_id }} · {{ b.merged_count }} 点</span>
          <span class="tag" :class="b.status === 'committed' ? 'ok' : 'bad'">
            {{ b.status === 'committed' ? '已合并' : '已回滚' }}
          </span>
        </button>
        <p v-if="!batches.length" class="muted">暂无合并批次</p>
      </aside>

      <section class="proof-pane" v-if="proof">
        <div class="proof-head">
          <h2>{{ proof.batch_id }}</h2>
          <span class="tag" :class="committed ? 'ok' : 'bad'">
            {{ committed ? '已合并' : '已回滚' }}
          </span>
          <span v-if="lastMode === 'failure'" class="hint">对照：失败未改正式线</span>
        </div>
        <p v-if="proof.error" class="err-line">{{ proof.error }}</p>

        <div class="stat-row">
          <div class="stat"><b>{{ proof.merged_count }}</b><span>并入点数</span></div>
          <div class="stat"><b>{{ proof.members?.length || 0 }}</b><span>成员段</span></div>
          <div class="stat"><b>{{ delta?.before }} → {{ delta?.after }}</b><span>投影前后</span></div>
        </div>

        <h3>主单</h3>
        <dl class="kv">
          <dt>运单</dt><dd>{{ proof.master?.id }}</dd>
          <dt>标题</dt><dd>{{ proof.master?.title }}</dd>
          <dt>投影</dt><dd>{{ proof.master?.status_proj }}</dd>
        </dl>

        <h3>成员段</h3>
        <table class="table">
          <thead>
            <tr><th>成员</th><th>标题</th><th>并入点数</th><th>归属主单</th></tr>
          </thead>
          <tbody>
            <tr v-for="m in proof.members" :key="m.id">
              <td>{{ m.id }}</td>
              <td>{{ m.title }}</td>
              <td>{{ m.merged_count }}</td>
              <td>{{ m.merged_into || '—' }}</td>
            </tr>
          </tbody>
        </table>

        <h3>前后投影</h3>
        <div class="proj-pair">
          <div class="proj-box">
            <span>并入前</span>
            <b>{{ proof.projection_before?.status_proj ?? '—' }}</b>
            <code>{{ proof.projection_before?.last_hop_id ?? '—' }}</code>
          </div>
          <div class="arrow" :class="{ dim: !committed }">→</div>
          <div class="proj-box">
            <span>并入后</span>
            <b>{{ proof.projection_after?.status_proj ?? '—' }}</b>
            <code>{{ proof.projection_after?.last_hop_id ?? '—' }}</code>
          </div>
        </div>

        <h3>并入点（{{ proof.merged_hop_ids?.length || 0 }}）</h3>
        <ul class="hop-ids">
          <li v-for="hid in proof.merged_hop_ids" :key="hid">{{ hid }}</li>
          <li v-if="!proof.merged_hop_ids?.length" class="muted">无（失败批次不写点）</li>
        </ul>
      </section>
      <section class="proof-pane muted" v-else>
        选择左侧批次查看合并证明
      </section>
    </div>
  </div>
</template>
