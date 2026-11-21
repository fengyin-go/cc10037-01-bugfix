import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 质量批次阻断回执 / 告警交接 ----

export type ThresholdJudgement = {
  rule_id: string
  rule_name: string
  rule_version: string
  blocked: boolean
  threshold_exceeded: boolean
  sample_size: number
  failed_rows: number
  failed_ratio: number
  threshold: number
  judgement: string
}

export type AlertEvent = {
  alert_id: string
  receipt_id: string
  batch_id: string
  rule_id: string
  rule_name: string
  rule_version: string
  level: string
  threshold: number
  threshold_judgement: string
  sample_size: number
  bad_rows: number
  bad_ratio: number
  on_call_group: string
  status: string
  delivery_attempts: number
  delivered_at: string | null
  created_at: string
  message: string
}

export type ReceiptView = {
  receipt_id: string
  batch_id: string
  pipeline_id: string
  target_table: string
  partition: string
  decision: string
  overall_pass: boolean
  created_at: string
  replay_count: number
  content_conflict: boolean
  threshold_judgements: ThresholdJudgement[]
  blocking_rules: ThresholdJudgement[]
  alerting_rules: ThresholdJudgement[]
  conclusion_confirmed: boolean
  resubmittable: boolean
  handoff_only: boolean
  alert_state: string
  alert_events: AlertEvent[]
  alert_count: number
  receiver_connected: boolean
  next_action: string
  batch_disposition: string
  operator_hint: string
}

export async function listQualityReceipts(): Promise<{ total: number; items: ReceiptView[] }> {
  const response = await request('/api/business/quality/receipts')
  if (!response.ok) throw new Error('回执列表读取失败')
  return await response.json()
}

export async function loadQualityReceipt(receiptId: string): Promise<ReceiptView> {
  const response = await request(`/api/business/quality/receipts/${receiptId}`)
  if (!response.ok) throw new Error('回执读取失败')
  return await response.json()
}

export async function registerReceiptAlerts(receiptId: string): Promise<{
  ok: boolean
  message: string
  alert_state: string
  next_action: string
  alert_events: AlertEvent[]
}> {
  const response = await request(`/api/business/quality/receipts/${receiptId}/alerts`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  if (!response.ok) throw new Error('告警登记失败')
  return await response.json()
}

export async function loadAlertInbox(): Promise<{
  total: number
  receiver_connected: boolean
  items: AlertEvent[]
}> {
  const response = await request('/api/business/alerts/inbox')
  if (!response.ok) throw new Error('告警面板读取失败')
  return await response.json()
}

export async function controlAlertReceiver(
  action: 'connect' | 'disconnect',
): Promise<{ ok: boolean; receiver_connected: boolean; message: string }> {
  const response = await request('/api/business/alerts/receiver', {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  if (!response.ok) throw new Error('告警接收侧操作失败')
  return await response.json()
}
