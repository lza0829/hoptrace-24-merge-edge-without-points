<script setup>
import { onMounted, ref } from 'vue'
import { api } from '../api.js'

const rows = ref([])
const batches = ref([])
onMounted(async () => {
  rows.value = await api.proofs()
  batches.value = await api.mergeBatches()
})
</script>

<template>
  <div class="page-card">
    <h1>证明</h1>
    <p class="lead">状态投影证明列表，与时间线末 hop 同源；合并批次证明见下方。</p>
    <table class="table">
      <thead>
        <tr><th>id</th><th>运单</th><th>末 hop</th><th>映射码</th><th>投影</th><th>时间</th></tr>
      </thead>
      <tbody>
        <tr v-for="r in rows" :key="r.id">
          <td>{{ r.id }}</td>
          <td>{{ r.waybill_id }}</td>
          <td>{{ r.last_hop_id }}</td>
          <td>{{ r.mapped_code }}</td>
          <td>{{ r.proj_value }}</td>
          <td>{{ r.created_at }}</td>
        </tr>
        <tr v-if="!rows.length"><td colspan="6" class="muted">暂无证明</td></tr>
      </tbody>
    </table>

    <h2 style="margin-top:28px">合并批次证明</h2>
    <table class="table">
      <thead>
        <tr><th>批次</th><th>主单</th><th>成员</th><th>并入点</th><th>状态</th><th>时间</th></tr>
      </thead>
      <tbody>
        <tr v-for="b in batches" :key="b.batch_id">
          <td>{{ b.batch_id }}</td>
          <td>{{ b.master_id }}</td>
          <td>{{ b.member_ids.join(', ') }}</td>
          <td>{{ b.merged_count }}</td>
          <td>{{ b.status === 'committed' ? '已合并' : '已回滚' }}</td>
          <td>{{ b.created_at }}</td>
        </tr>
        <tr v-if="!batches.length"><td colspan="6" class="muted">暂无合并批次</td></tr>
      </tbody>
    </table>
    <p class="muted" style="margin-top:10px">
      逐批明细（前后投影 / 并入点清单）见 <router-link to="/merge" style="text-decoration:underline">并单页</router-link>。
    </p>
  </div>
  <!-- pad-24 continuity -->
</template>
