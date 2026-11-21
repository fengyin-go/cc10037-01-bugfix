<template>
  <section class="receipt-panel" data-testid="quality-receipt-panel">
    <header class="receipt-head">
      <div>
        <h3>质量批次阻断回执</h3>
        <p class="page-desc">
          回执里的每条规则单独判定阈值；告警依据只取越线规则，不使用批次汇总坏行率。
        </p>
      </div>
      <div class="receipt-head-actions">
        <span class="receiver-state" :data-connected="String(receiverConnected)">
          接收侧：{{ receiverConnected ? '已连接' : '已断开' }}
        </span>
        <button
          class="btn"
          data-testid="toggle-receiver"
          type="button"
          @click="toggleReceiver"
        >
          {{ receiverConnected ? '断开接收侧' : '恢复接收侧' }}
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>回执</th>
          <th>批次</th>
          <th>门禁结论</th>
          <th>越线规则</th>
          <th>告警交接</th>
          <th>值班处置</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="receipt in receipts"
          :key="receipt.receipt_id"
          :data-testid="`receipt-${receipt.batch_id}`"
        >
          <td>
            <button class="link" type="button" @click="selectReceipt(receipt)">
              {{ receipt.receipt_id }}
            </button>
          </td>
          <td>{{ receipt.batch_id }}</td>
          <td>{{ receipt.decision }}</td>
          <td>
            <span v-if="!receipt.alerting_rules.length">无</span>
            <span
              v-for="rule in receipt.alerting_rules"
              :key="rule.rule_id"
              class="rule-chip"
            >
              {{ rule.rule_name }}@{{ rule.rule_version }}（{{ rule.failed_rows }}/{{ rule.sample_size }}）
            </span>
          </td>
          <td :data-alert-state="receipt.alert_state">
            {{ receipt.alert_state }}
            <span v-if="receipt.handoff_only" class="handoff-tag">待交接</span>
          </td>
          <td>
            <span>{{ receipt.batch_disposition }}</span>
            <button
              v-if="receipt.handoff_only"
              class="link"
              type="button"
              :data-testid="`handoff-${receipt.batch_id}`"
              @click="handoff(receipt)"
            >
              补投告警
            </button>
          </td>
        </tr>
        <tr v-if="!receipts.length">
          <td colspan="6" class="empty-state">暂无质量批次回执，先执行一次质量门禁</td>
        </tr>
      </tbody>
    </table>

    <div v-if="selected" class="receipt-detail" data-testid="receipt-detail">
      <h4>阈值判定明细 · {{ selected.receipt_id }}</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>规则</th>
            <th>版本</th>
            <th>样本数</th>
            <th>坏行数</th>
            <th>坏行率</th>
            <th>适用阈值</th>
            <th>判定</th>
            <th>结论</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in selected.threshold_judgements"
            :key="item.rule_id"
            :data-testid="`judgement-${item.rule_id}`"
            :data-decision="item.blocked ? 'BLOCK' : item.threshold_exceeded ? 'ALERT' : 'PASS'"
          >
            <td>{{ item.rule_name }}</td>
            <td>{{ item.rule_version }}</td>
            <td>{{ item.sample_size }}</td>
            <td>{{ item.failed_rows }}</td>
            <td>{{ (item.failed_ratio * 100).toFixed(2) }}%</td>
            <td>{{ (item.threshold * 100).toFixed(2) }}%</td>
            <td>{{ item.judgement }}</td>
            <td>{{ item.blocked ? '阻断' : item.threshold_exceeded ? '告警' : '通过' }}</td>
          </tr>
        </tbody>
      </table>

      <h4>登记给值班组的告警事件</h4>
      <ul class="alert-event-list">
        <li
          v-for="event in selected.alert_events"
          :key="event.alert_id"
          :data-testid="`alert-event-${event.rule_id}`"
          :data-status="event.status"
        >
          {{ event.level }} · {{ event.rule_name }}@{{ event.rule_version }}
          · {{ event.bad_rows }}/{{ event.sample_size }} 行（{{
            (event.bad_ratio * 100).toFixed(2)
          }}%） · {{ event.status }} · {{ event.on_call_group }}
        </li>
        <li v-if="!selected.alert_events.length" class="empty-state">该回执没有越线规则</li>
      </ul>
      <p class="operator-hint">{{ selected.operator_hint }}</p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  controlAlertReceiver,
  listQualityReceipts,
  loadAlertInbox,
  registerReceiptAlerts,
} from '@/api/backend-service'
import type { ReceiptView } from '@/api/backend-service'

const receipts = ref<ReceiptView[]>([])
const selected = ref<ReceiptView | null>(null)
const receiverConnected = ref(true)
const errorMessage = ref('')

function selectReceipt(receipt: ReceiptView) {
  selected.value = receipt
}

async function reload() {
  errorMessage.value = ''
  try {
    const [payload, inbox] = await Promise.all([listQualityReceipts(), loadAlertInbox()])
    receipts.value = payload.items
    receiverConnected.value = inbox.receiver_connected
    if (selected.value) {
      selected.value =
        payload.items.find((item) => item.receipt_id === selected.value?.receipt_id) ?? null
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执读取失败'
  }
}

async function toggleReceiver() {
  errorMessage.value = ''
  try {
    const result = await controlAlertReceiver(receiverConnected.value ? 'disconnect' : 'connect')
    receiverConnected.value = result.receiver_connected
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接收侧操作失败'
  }
}

async function handoff(receipt: ReceiptView) {
  errorMessage.value = ''
  try {
    await registerReceiptAlerts(receipt.receipt_id)
    await reload()
    selected.value = receipts.value.find((item) => item.receipt_id === receipt.receipt_id) ?? null
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '告警登记失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.receipt-panel {
  margin-top: 20px;
  padding: 16px;
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
}
.receipt-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 12px;
}
.receipt-head h3 {
  margin: 0;
  font-size: 15px;
}
.receipt-head-actions {
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 12px;
  color: var(--muted);
}
.receiver-state[data-connected='false'] {
  color: #b42318;
}
.rule-chip {
  display: inline-block;
  margin: 2px 6px 2px 0;
  padding: 1px 6px;
  border-radius: 4px;
  background: #fff4e5;
  color: #b45309;
  font-size: 12px;
}
.handoff-tag {
  margin-left: 6px;
  color: #b42318;
  font-size: 12px;
}
.receipt-detail {
  margin-top: 16px;
  padding-top: 12px;
  border-top: 1px solid var(--border);
}
.receipt-detail h4 {
  margin: 8px 0;
  font-size: 13px;
}
.alert-event-list {
  margin: 0;
  padding-left: 18px;
  font-size: 12px;
}
.alert-event-list li[data-status='PENDING'] {
  color: #b42318;
}
.operator-hint {
  margin: 10px 0 0;
  font-size: 12px;
  color: var(--muted);
}
</style>
