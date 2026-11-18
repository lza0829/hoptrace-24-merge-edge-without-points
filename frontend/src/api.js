async function req(path, opts = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    ...opts,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || res.statusText)
  }
  return res.json()
}

export const api = {
  health: () => req('/api/health'),
  overview: () => req('/api/overview'),
  waybills: () => req('/api/waybills'),
  timeline: (id) => req(`/api/waybills/${encodeURIComponent(id)}/timeline`),
  quarantine: () => req('/api/quarantine'),
  sidecar: () => req('/api/sidecar'),
  ingest: () => req('/api/ingest/run', { method: 'POST' }),
  proofs: (waybillId) =>
    req(waybillId ? `/api/status-proofs?waybill_id=${encodeURIComponent(waybillId)}` : '/api/status-proofs'),
  mergeRun: (masterId, memberIds) =>
    req('/api/merge/run', {
      method: 'POST',
      body: JSON.stringify({ master_id: masterId, member_ids: memberIds }),
    }),
  mergeFixture: (mode) =>
    req(`/api/merge/fixture?mode=${encodeURIComponent(mode)}`, { method: 'POST' }),
  mergeBatches: () => req('/api/merge/batches'),
  mergeBatch: (batchId) => req(`/api/merge/batches/${encodeURIComponent(batchId)}`),
  mergeMemberGates: (masterId) =>
    req(masterId
      ? `/api/merge/member-gates?master_id=${encodeURIComponent(masterId)}`
      : '/api/merge/member-gates'),
  mergeMemberGate: (memberId) =>
    req(`/api/merge/member-gates/${encodeURIComponent(memberId)}`),
}
