<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>运营概览</h2>
        <p class="page-desc">汇总各业务模块的关键指标，先看总量再看异常。</p>
      </div>
    </header>
    <div class="stat-row">
      <article v-for="card in cards" :key="card.label" class="stat-card">
        <span class="stat-label">{{ card.label }}</span>
        <strong class="stat-value">{{ card.value }}</strong>
      </article>
    </div>
    <table class="data-table">
      <thead>
        <tr><th>业务模块</th><th>今日新增</th><th>待处理</th><th>异常量</th></tr>
      </thead>
      <tbody>
        <tr v-for="row in moduleRows" :key="row.name">
          <td>{{ row.name }}</td>
          <td>{{ row.created }}</td>
          <td>{{ row.pending }}</td>
          <td>{{ row.abnormal }}</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

type Overview = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

const cards = ref<Overview['cards']>([])
const moduleRows = ref<Overview['modules']>([])

onMounted(async () => {
  try {
    const payload = await fetchJson<Overview>('/api/overview')
    cards.value = payload.cards
    moduleRows.value = payload.modules
  } catch {
    cards.value = [{"label": "业务模块", "value": 0}, {"label": "今日新增", "value": 0}]
    moduleRows.value = [{"name": "流水线定义", "created": 0, "pending": 0, "abnormal": 0}, {"name": "任务节点", "created": 0, "pending": 0, "abnormal": 0}, {"name": "质量规则", "created": 0, "pending": 0, "abnormal": 0}, {"name": "质量执行", "created": 0, "pending": 0, "abnormal": 0}, {"name": "依赖调度", "created": 0, "pending": 0, "abnormal": 0}, {"name": "滑动窗口", "created": 0, "pending": 0, "abnormal": 0}, {"name": "回刷补偿", "created": 0, "pending": 0, "abnormal": 0}, {"name": "水位追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "资源池", "created": 0, "pending": 0, "abnormal": 0}, {"name": "血缘图谱", "created": 0, "pending": 0, "abnormal": 0}, {"name": "异常捕获", "created": 0, "pending": 0, "abnormal": 0}, {"name": "模板仓库", "created": 0, "pending": 0, "abnormal": 0}, {"name": "分区扫描", "created": 0, "pending": 0, "abnormal": 0}, {"name": "配置迁移", "created": 0, "pending": 0, "abnormal": 0}, {"name": "Schema演进", "created": 0, "pending": 0, "abnormal": 0}, {"name": "回填追踪", "created": 0, "pending": 0, "abnormal": 0}, {"name": "成本分析", "created": 0, "pending": 0, "abnormal": 0}, {"name": "告警策略", "created": 0, "pending": 0, "abnormal": 0}]
  }
})
</script>
