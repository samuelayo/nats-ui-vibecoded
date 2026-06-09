<script setup>
import { computed, nextTick, onMounted, reactive, ref, watch } from 'vue'
import * as echarts from 'echarts'
import {
  Connect,
  Disconnect,
  GetServerInfo,
  ListStreams,
  GetStreamDetail,
  ListAllConsumers,
  ScanMessagesStream,
  ConsumerCandidateMessages,
  RepublishMessage,
  ListKeyValueBuckets,
  ListObjectStores,
  ListProfiles,
  SaveProfile,
  DeleteProfile,
  ConnectProfile,
  CorePublish,
  SubscribeCore,
  UnsubscribeCore,
  RunNatsCLI,
} from '../wailsjs/go/main/App'
import { EventsOn, Quit, WindowMinimise, WindowToggleMaximise } from '../wailsjs/runtime/runtime'

const pages = [
  ['dashboard', 'Dashboard'],
  ['pubsub', 'Pub/Sub'],
  ['streams', 'Streams'],
  ['consumers', 'Consumers'],
  ['messages', 'Message Browse'],
  ['cli', 'CLI'],
  ['storage', 'Storage'],
]

const state = reactive({
  page: 'dashboard',
  busy: false,
  scanning: false,
  scanProgress: { matched: 0, scanned: 0, current: 0 },
  scanSession: '',
  pubsubSession: '',
  error: '',
  connected: false,
  status: 'Disconnected',
  connect: {
    url: localStorage.getItem('nats.url') || 'nats://localhost:3222',
    username: localStorage.getItem('nats.username') || '',
    password: '',
    token: '',
    credsPath: '',
  },
  profiles: [],
  newProfile: {
    name: '',
    url: localStorage.getItem('nats.url') || 'nats://localhost:3222',
    username: localStorage.getItem('nats.username') || '',
    password: '',
    token: '',
    credsPath: '',
  },
  server: null,
  streams: [],
  streamQuery: '',
  selectedStream: null,
  streamDetail: null,
  consumers: [],
  messages: [],
  selectedMessage: null,
  selectedConsumer: null,
  candidateMessages: [],
  filters: {
    subjectContains: '',
    payloadContains: '',
    headerKey: '',
    headerValue: '',
    limit: 100,
    maxProbes: 5000,
    direction: 'backward',
    startSeq: 0,
  },
  kv: [],
  objects: [],
  pubsub: {
    subject: '>',
    queue: '',
    publishSubject: '',
    publishPayload: '',
    publishHeaders: '',
    subscribed: false,
    messages: [],
  },
  cli: {
    command: 'stream ls',
    useConnection: true,
    timeoutSeconds: 30,
    running: false,
    results: [],
  },
})

const streamChart = ref(null)
const shardChart = ref(null)
const serverChart = ref(null)
const consumerChart = ref(null)
const detailChart = ref(null)

let charts = new Map()

function palette() {
  return ['#38d996', '#ffb020', '#ff3f8f', '#5aa9ff', '#b981ff', '#38f3ff']
}

function chart(refEl, option) {
  if (!refEl.value) return
  let c = charts.get(refEl.value)
  if (!c) {
    c = echarts.init(refEl.value, 'dark')
    charts.set(refEl.value, c)
  }
  c.setOption(option, true)
}

function chartBase() {
  return {
    backgroundColor: 'transparent',
    color: palette(),
    textStyle: { color: '#d9e7ff', fontFamily: 'Aptos, Segoe UI, sans-serif' },
    tooltip: { trigger: 'axis', backgroundColor: '#081a31', borderColor: '#24537c', textStyle: { color: '#f4fbff' } },
    grid: { left: 44, right: 22, top: 32, bottom: 42 },
  }
}

const filteredStreams = computed(() => {
  const q = state.streamQuery.trim().toLowerCase()
  if (!q) return state.streams
  return state.streams.filter((s) => s.name.toLowerCase().includes(q) || (s.subjects || []).join(' ').toLowerCase().includes(q))
})

const totals = computed(() => {
  const t = { messages: 0, deleted: 0, bytes: 0, subjects: 0, consumers: 0, streams: state.streams.length }
  for (const s of state.streams) {
    t.messages += s.messages || 0
    t.deleted += s.deleted || 0
    t.bytes += s.bytes || 0
    t.subjects += s.numSubjects || 0
    t.consumers += s.consumers || 0
  }
  return t
})

const risk = computed(() => {
  const t = { pending: 0, ack: 0, redelivered: 0 }
  for (const c of state.consumers) {
    t.pending += c.numPending || 0
    t.ack += c.numAckPending || 0
    t.redelivered += c.numRedelivered || 0
  }
  return t
})

async function run(fn) {
  state.busy = true
  state.error = ''
  try {
    return await fn()
  } catch (e) {
    state.error = e?.message || String(e)
    throw e
  } finally {
    state.busy = false
  }
}

async function connect() {
  await run(async () => {
    localStorage.setItem('nats.url', state.connect.url)
    localStorage.setItem('nats.username', state.connect.username)
    localStorage.removeItem('nats.password')
    state.server = await Connect(state.connect)
    state.connected = true
    state.status = `Connected to ${state.connect.url}`
    await refreshAll()
  })
}

async function loadProfiles() {
  try {
    state.profiles = await ListProfiles()
  } catch (e) {
    state.error = e?.message || String(e)
  }
}

async function connectProfile(profile) {
  await run(async () => {
    state.server = await ConnectProfile(profile.id)
    state.connected = true
    state.status = `Connected to ${profile.name}`
    state.connect.url = profile.url
    state.connect.username = profile.username
    await refreshAll()
  })
}

async function saveProfile() {
  await run(async () => {
    await SaveProfile(state.newProfile)
    state.newProfile = {
      name: '',
      url: state.connect.url || 'nats://localhost:3222',
      username: state.connect.username || '',
      password: '',
      token: '',
      credsPath: '',
    }
    await loadProfiles()
  })
}

async function deleteProfile(profile) {
  if (!confirm(`Delete profile ${profile.name}?`)) return
  await run(async () => {
    await DeleteProfile(profile.id)
    await loadProfiles()
  })
}

async function disconnect() {
  await Disconnect()
  state.connected = false
  state.status = 'Disconnected'
  state.server = null
  state.streams = []
  state.consumers = []
  state.messages = []
}

async function refreshAll() {
  if (!state.connected) return
  await run(async () => {
    const [server, streams] = await Promise.all([GetServerInfo(), ListStreams()])
    state.server = server
    state.streams = streams || []
    await nextTick()
    drawCharts()
  })
  ListAllConsumers().then((consumers) => {
    state.consumers = consumers || []
    nextTick(drawCharts)
  }).catch((e) => {
    state.error = e?.message || String(e)
  })
}

async function openStream(stream) {
  state.selectedStream = stream
  state.page = 'stream'
  await run(async () => {
    state.streamDetail = await GetStreamDetail(stream.name)
    await nextTick()
    drawDetailChart(stream)
  })
}

async function browseStream(stream = state.selectedStream) {
  if (!stream) return
  state.selectedStream = stream
  state.page = 'messages'
  await scanMessages()
}

async function scanMessages() {
  if (!state.selectedStream) return
  const session = `${Date.now()}-${Math.random().toString(16).slice(2)}`
  state.scanSession = session
  state.messages = []
  state.scanning = true
  state.scanProgress = { matched: 0, scanned: 0, current: 0 }
  await run(async () => {
    await ScanMessagesStream(state.selectedStream.name, state.filters, session)
  })
}

async function openConsumer(consumer) {
  state.selectedConsumer = consumer
  await run(async () => {
    state.candidateMessages = await ConsumerCandidateMessages(consumer, 120)
  })
}

function openCandidateMessage(message) {
  state.selectedMessage = message
  state.selectedConsumer = null
}

async function republish(message) {
  if (!message) return
  if (!confirm('Republish this exact payload and headers? Use carefully on scheduler subjects.')) return
  await run(async () => RepublishMessage(message.subject, message.data, message.headers || {}))
}

async function loadStorage() {
  state.page = 'storage'
  await run(async () => {
    const [kv, objects] = await Promise.all([ListKeyValueBuckets(), ListObjectStores()])
    state.kv = kv || []
    state.objects = objects || []
  })
}

async function togglePubSub() {
  if (!state.pubsub.subscribed) {
    const session = `${Date.now()}-${Math.random().toString(16).slice(2)}`
    state.pubsubSession = session
    state.pubsub.messages = []
    await run(async () => {
      await SubscribeCore(state.pubsub.subject || '>', state.pubsub.queue || '', session)
      state.pubsub.subscribed = true
    })
    return
  }
  await run(async () => {
    await UnsubscribeCore(state.pubsubSession)
    state.pubsub.subscribed = false
  })
}

function parseHeaders(text) {
  const headers = {}
  for (const line of (text || '').split('\n')) {
    const trimmed = line.trim()
    if (!trimmed) continue
    const idx = trimmed.indexOf(':')
    if (idx <= 0) continue
    headers[trimmed.slice(0, idx).trim()] = trimmed.slice(idx + 1).trim()
  }
  return headers
}

async function publishCore() {
  await run(async () => {
    await CorePublish(state.pubsub.publishSubject, state.pubsub.publishPayload, parseHeaders(state.pubsub.publishHeaders))
  })
}

async function runCLICommand() {
  const command = state.cli.command.trim()
  if (!command || state.cli.running) return
  state.cli.running = true
  state.error = ''
  try {
    const result = await RunNatsCLI({
      command,
      useConnection: state.cli.useConnection,
      url: state.connect.url,
      username: state.connect.username,
      password: state.connect.password,
      token: state.connect.token,
      credsPath: state.connect.credsPath,
      timeoutSeconds: Number(state.cli.timeoutSeconds) || 30,
    })
    state.cli.results.unshift(result)
    state.cli.results = state.cli.results.slice(0, 25)
  } catch (e) {
    state.error = e?.message || String(e)
    state.cli.results.unshift({
      command,
      stdout: '',
      stderr: state.error,
      exitCode: -1,
      durationMillis: 0,
      startedAt: new Date().toISOString(),
    })
  } finally {
    state.cli.running = false
  }
}

function useCLIExample(command) {
  state.cli.command = command
}

function drawCharts() {
  drawServerChart()
  drawStreamChart()
  drawShardChart()
  drawConsumerChart()
}

function drawServerChart() {
  if (!state.server) return
  chart(serverChart, {
    ...chartBase(),
    tooltip: { trigger: 'item' },
    series: [{
      type: 'gauge',
      min: 0,
      max: Math.max(1, state.server.apiRequests || 1),
      progress: { show: true, width: 16 },
      axisLine: { lineStyle: { width: 16, color: [[0.75, '#235a8a'], [1, '#1d2c48']] } },
      pointer: { show: false },
      detail: { formatter: `${state.server.apiErrors || 0} API errors`, color: '#ffffff', fontSize: 18 },
      data: [{ value: state.server.apiErrors || 0 }],
    }],
  })
}

function drawStreamChart() {
  const top = [...state.streams].sort((a, b) => {
    const ash = schedulerShard(a.name)
    const bsh = schedulerShard(b.name)
    if (ash !== null && bsh !== null) return ash - bsh
    if (ash !== null) return -1
    if (bsh !== null) return 1
    return a.name.localeCompare(b.name)
  })
  const visible = top.length > 28 ? Math.max(18, (28 / top.length) * 100) : 100
  chart(streamChart, {
    ...chartBase(),
    legend: { top: 0, textStyle: { color: '#c9d8ef' }, selectedMode: true },
    xAxis: { type: 'category', data: top.map((s) => shortName(s.name)), axisLabel: { rotate: 35, interval: 0 } },
    yAxis: { type: 'value', splitLine: { lineStyle: { color: '#143557' } } },
    dataZoom: [
      { type: 'slider', xAxisIndex: 0, start: 0, end: visible, bottom: 4, height: 18, borderColor: '#1a4569', fillerColor: 'rgba(56,243,255,.16)' },
      { type: 'inside', xAxisIndex: 0 },
    ],
    series: [
      { name: 'Active', type: 'line', smooth: true, symbolSize: 7, areaStyle: { opacity: 0.16 }, data: top.map((s) => s.messages || 0) },
      { name: 'Deleted', type: 'line', smooth: true, symbolSize: 7, areaStyle: { opacity: 0.12 }, data: top.map((s) => s.deleted || 0) },
      { name: 'Subjects', type: 'line', smooth: true, symbolSize: 7, areaStyle: { opacity: 0.08 }, data: top.map((s) => s.numSubjects || 0) },
    ],
  })
}

function schedulerShard(name) {
  const match = /^SCHEDULER_(\d+)$/.exec(name || '')
  return match ? Number(match[1]) : null
}

function drawShardChart() {
  const shards = [...state.streams]
    .filter((s) => /^SCHEDULER_\d+$/.test(s.name))
    .sort((a, b) => Number(a.name.split('_')[1]) - Number(b.name.split('_')[1]))
  chart(shardChart, {
    ...chartBase(),
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, formatter: (items) => {
      const idx = items[0]?.dataIndex ?? 0
      const s = shards[idx]
      return `<b>${s.name}</b><br/>Active: ${s.messages}<br/>Deleted: ${s.deleted}<br/>Subjects: ${s.numSubjects}`
    }},
    xAxis: { type: 'category', data: shards.map((s) => s.name.split('_')[1]), axisLabel: { interval: 3 } },
    yAxis: { type: 'value', splitLine: { lineStyle: { color: '#143557' } } },
    series: [
      { name: 'Active', type: 'bar', stack: 'shape', data: shards.map((s) => s.messages || 0) },
      { name: 'Deleted', type: 'bar', stack: 'shape', data: shards.map((s) => s.deleted || 0) },
    ],
  })
}

function drawConsumerChart() {
  const top = [...state.consumers].sort((a, b) => {
    const ar = (a.numAckPending || 0) + (a.numRedelivered || 0) + (a.numPending || 0)
    const br = (b.numAckPending || 0) + (b.numRedelivered || 0) + (b.numPending || 0)
    if (ar !== br) return br - ar
    return (a.name || '').localeCompare(b.name || '')
  })
  const visible = top.length > 18 ? Math.max(8, (18 / top.length) * 100) : 100
  chart(consumerChart, {
    ...chartBase(),
    legend: { top: 0, textStyle: { color: '#c9d8ef' } },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      formatter: (items) => {
        const i = items[0]?.dataIndex ?? 0
        const c = top[i]
        if (!c) return ''
        const total = (c.numAckPending || 0) + (c.numRedelivered || 0) + (c.numPending || 0)
        return `<b>${c.name}</b><br/>Stream: ${c.streamName}<br/>Filter: ${c.filterSubject || '-'}<br/>Pending: ${c.numPending || 0}<br/>Ack pending: ${c.numAckPending || 0}<br/>Redelivered: ${c.numRedelivered || 0}<br/>Total pressure: ${total}`
      },
    },
    grid: { left: 190, right: 34, top: 42, bottom: 42 },
    xAxis: {
      type: 'value',
      min: 0,
      minInterval: 1,
      splitLine: { lineStyle: { color: '#143557' } },
      axisLabel: { color: '#c9d8ef' },
    },
    yAxis: {
      type: 'category',
      inverse: true,
      data: top.map((c) => shortName(c.name, 28)),
      axisLabel: { color: '#dbe8ff', width: 170, overflow: 'truncate' },
    },
    dataZoom: [
      { type: 'slider', yAxisIndex: 0, right: 0, start: 0, end: visible, borderColor: '#1a4569', fillerColor: 'rgba(56,243,255,.16)' },
      { type: 'inside', yAxisIndex: 0 },
    ],
    series: [
      {
        name: 'Healthy',
        type: 'scatter',
        symbolSize: 8,
        data: top.map((c, i) => {
          const total = (c.numAckPending || 0) + (c.numRedelivered || 0) + (c.numPending || 0)
          return total === 0 ? [0, i] : null
        }),
        itemStyle: { color: '#38d996' },
      },
      { name: 'Ack Pending', type: 'bar', stack: 'pressure', barWidth: 12, data: top.map((c) => c.numAckPending || 0) },
      { name: 'Redelivered', type: 'bar', stack: 'pressure', barWidth: 12, data: top.map((c) => c.numRedelivered || 0) },
      { name: 'Pending', type: 'bar', stack: 'pressure', barWidth: 12, data: top.map((c) => c.numPending || 0) },
    ],
  })
}

function drawDetailChart(stream) {
  if (!stream) return
  chart(detailChart, {
    ...chartBase(),
    tooltip: { trigger: 'item' },
    radar: {
      indicator: [
        { name: 'Active', max: Math.max(stream.messages, 1) },
        { name: 'Deleted', max: Math.max(stream.deleted, 1) },
        { name: 'Subjects', max: Math.max(stream.numSubjects, 1) },
        { name: 'Consumers', max: Math.max(stream.consumers, 1) },
        { name: 'MB', max: Math.max(stream.bytes / 1024 / 1024, 1) },
      ],
      splitLine: { lineStyle: { color: '#164061' } },
      axisName: { color: '#d9e7ff' },
    },
    series: [{
      type: 'radar',
      areaStyle: { opacity: 0.28 },
      data: [{ value: [stream.messages, stream.deleted, stream.numSubjects, stream.consumers, stream.bytes / 1024 / 1024], name: stream.name }],
    }],
  })
}

function shortName(v, max = 18) {
  if (!v) return ''
  if (v.length <= max) return v
  return v.slice(0, Math.floor(max / 2)) + '...' + v.slice(-Math.floor(max / 2))
}

function bytes(v) {
  if (!v) return '0 B'
  if (v > 1024 ** 3) return `${(v / 1024 ** 3).toFixed(1)} GB`
  if (v > 1024 ** 2) return `${(v / 1024 ** 2).toFixed(1)} MB`
  if (v > 1024) return `${(v / 1024).toFixed(1)} KB`
  return `${v} B`
}

function fmtTime(v) {
  if (!v) return '-'
  return new Date(v).toLocaleString()
}

function pretty(text) {
  if (!text) return '(empty)'
  try { return JSON.stringify(JSON.parse(text), null, 2) } catch { return text }
}

watch(() => [state.streams, state.consumers, state.page], () => nextTick(drawCharts), { deep: true })
function showStreams() {
  state.page = 'streams'
}

function showConsumers() {
  state.page = 'consumers'
}

function showRedelivered() {
  state.page = 'consumers'
  nextTick(drawConsumerChart)
}

function showStorage() {
  loadStorage()
}

onMounted(async () => {
  EventsOn('pubsub:message', (payload) => {
    if (!payload || payload.session !== state.pubsubSession || !payload.message) return
    state.pubsub.messages.unshift(payload.message)
    if (state.pubsub.messages.length > 500) state.pubsub.messages.pop()
  })
  EventsOn('scan:message', (payload) => {
    if (!payload || payload.session !== state.scanSession || !payload.message) return
    state.messages.push(payload.message)
  })
  EventsOn('scan:progress', (payload) => {
    if (!payload || payload.session !== state.scanSession) return
    state.scanProgress = {
      matched: payload.matched || 0,
      scanned: payload.scanned || 0,
      current: payload.current || 0,
    }
    if (payload.done) {
      state.scanning = false
    }
  })
  await loadProfiles()
  await nextTick(drawCharts)
})
</script>

<template>
  <main class="app-shell">
    <div class="window-titlebar">
      <div class="window-title">NATS Observatory</div>
      <div class="window-controls">
        <button @click="WindowMinimise()">-</button>
        <button @click="WindowToggleMaximise()">□</button>
        <button class="close" @click="Quit()">x</button>
      </div>
    </div>
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark"><span></span><span></span><span></span><span></span></div>
        <div>
          <h1>NATS Observatory</h1>
          <p>{{ state.status }}</p>
        </div>
      </div>

      <nav>
        <button v-for="[key, label] in pages" :key="key" :class="{ active: state.page === key }" @click="key === 'storage' ? loadStorage() : state.page = key">
          {{ label }}
        </button>
      </nav>

      <section class="connect-card">
        <label>Server</label>
        <input v-model="state.connect.url" placeholder="nats://localhost:4222" />
        <div class="two">
          <input v-model="state.connect.username" placeholder="username" />
          <input v-model="state.connect.password" placeholder="password" type="password" />
        </div>
        <button v-if="!state.connected" class="primary" @click="connect">Connect</button>
        <button v-else class="danger" @click="disconnect">Disconnect</button>

        <div class="profile-list" v-if="state.profiles.length">
          <label>Saved profiles</label>
          <div v-for="profile in state.profiles" :key="profile.id" class="profile-row">
            <button class="profile-main" @click="connectProfile(profile)">
              <strong>{{ profile.name }}</strong>
              <span>{{ profile.url }}</span>
            </button>
            <button class="profile-delete" @click="deleteProfile(profile)">x</button>
          </div>
        </div>

        <details class="profile-form">
          <summary>Save profile</summary>
          <input v-model="state.newProfile.name" placeholder="profile name" />
          <input v-model="state.newProfile.url" placeholder="nats://host:4222" />
          <div class="two">
            <input v-model="state.newProfile.username" placeholder="username" />
            <input v-model="state.newProfile.password" placeholder="password" type="password" />
          </div>
          <input v-model="state.newProfile.token" placeholder="token" />
          <input v-model="state.newProfile.credsPath" placeholder="creds path" />
          <button class="primary" @click="saveProfile">Save to shared DB</button>
        </details>
      </section>
    </aside>

    <section class="workspace">
      <header class="topbar">
        <div>
          <p class="eyebrow">JetStream control room</p>
          <h2>{{ state.page === 'stream' ? state.selectedStream?.name : pages.find(p => p[0] === state.page)?.[1] || 'Stream' }}</h2>
        </div>
        <div class="actions">
          <button @click="refreshAll" :disabled="!state.connected">Refresh</button>
        </div>
      </header>
      <div v-if="state.busy || state.scanning" class="loading-rail"><span></span></div>

      <div v-if="state.error" class="error">{{ state.error }}</div>

      <section v-if="state.page === 'dashboard'" class="page">
        <div class="hero-grid">
          <article class="metric xl clickable" @click="showStreams">
            <span>Streams</span><strong>{{ totals.streams }}</strong><em>{{ totals.messages.toLocaleString() }} retained messages</em>
          </article>
          <article class="metric clickable" @click="showConsumers"><span>Consumers</span><strong>{{ totals.consumers }}</strong><em>{{ risk.ack }} ack pending</em></article>
          <article class="metric clickable" @click="showStorage"><span>Storage</span><strong>{{ bytes(totals.bytes) }}</strong><em>{{ totals.subjects.toLocaleString() }} subjects</em></article>
          <article class="metric hot clickable" @click="showRedelivered"><span>Redelivered</span><strong>{{ risk.redelivered }}</strong><em>retry pressure</em></article>
        </div>
        <div class="chart-grid">
          <article class="panel wide"><h3>Stream Mass</h3><div ref="streamChart" class="chart"></div></article>
          <article class="panel"><h3>API Error Pulse</h3><div ref="serverChart" class="chart"></div></article>
          <article class="panel wide"><h3>Scheduler Shards</h3><div ref="shardChart" class="chart tall"></div></article>
          <article class="panel"><h3>Consumer Pressure</h3><div ref="consumerChart" class="chart tall"></div></article>
        </div>
      </section>

      <section v-if="state.page === 'pubsub'" class="page">
        <div class="chart-grid">
          <article class="panel">
            <h3>Subscribe</h3>
            <div class="form-grid">
              <input v-model="state.pubsub.subject" placeholder="subject, e.g. > or foo.*" />
              <input v-model="state.pubsub.queue" placeholder="queue group, optional" />
              <button :class="state.pubsub.subscribed ? 'danger' : 'primary'" @click="togglePubSub">
                {{ state.pubsub.subscribed ? 'Stop' : 'Subscribe' }}
              </button>
            </div>
          </article>
          <article class="panel">
            <h3>Publish</h3>
            <div class="form-grid">
              <input v-model="state.pubsub.publishSubject" placeholder="publish subject" />
              <textarea v-model="state.pubsub.publishPayload" placeholder="payload"></textarea>
              <textarea v-model="state.pubsub.publishHeaders" placeholder="headers, one per line: Key: Value"></textarea>
              <button class="primary" @click="publishCore">Publish</button>
            </div>
          </article>
        </div>
        <div class="table-card">
          <table>
            <thead><tr><th>Received</th><th>Subject</th><th>Reply</th><th>Size</th><th>Payload</th></tr></thead>
            <tbody>
              <tr v-for="(m, i) in state.pubsub.messages" :key="i" @click="state.selectedMessage = { sequence: 0, subject: m.subject, time: m.received, data: m.data, headers: m.headers, size: m.size }">
                <td>{{ fmtTime(m.received) }}</td><td class="link">{{ m.subject }}</td><td>{{ m.reply }}</td><td>{{ m.size }}</td><td>{{ m.data }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section v-if="state.page === 'streams'" class="page">
        <div class="toolbar">
          <input v-model="state.streamQuery" placeholder="Filter streams or subjects…" />
          <button @click="refreshAll">Refresh</button>
        </div>
        <div class="table-card">
          <table>
            <thead><tr><th>Name</th><th>Subjects</th><th>Msgs</th><th>Deleted</th><th>Unique Subjects</th><th>Bytes</th><th>Consumers</th><th>Flags</th></tr></thead>
            <tbody>
              <tr v-for="s in filteredStreams" :key="s.name" @click="openStream(s)">
                <td class="link">{{ s.name }}</td><td>{{ (s.subjects || []).join(', ') }}</td><td>{{ s.messages }}</td><td>{{ s.deleted }}</td><td>{{ s.numSubjects }}</td><td>{{ bytes(s.bytes) }}</td><td>{{ s.consumers }}</td>
                <td><span v-if="s.allowDirect">D</span><span v-if="s.allowRollup"> R</span><span v-if="s.allowMsgSched"> S</span><span v-if="s.allowAtomic"> A</span><span v-if="s.maxMsgsPerSub === 1"> M1</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section v-if="state.page === 'stream'" class="page">
        <div class="toolbar">
          <button @click="state.page = 'streams'">Back</button>
          <button @click="browseStream()">Browse Messages</button>
        </div>
        <div class="warnings" v-if="state.streamDetail?.warnings?.length">
          <div v-for="w in state.streamDetail.warnings" :key="w">! {{ w }}</div>
        </div>
        <div class="chart-grid">
          <article class="panel"><h3>Shape Fingerprint</h3><div ref="detailChart" class="chart"></div></article>
          <article class="panel code"><h3>Stream Config</h3><pre>{{ state.streamDetail?.configJSON }}</pre></article>
          <article class="panel code"><h3>Stream State</h3><pre>{{ state.streamDetail?.stateJSON }}</pre></article>
        </div>
        <div class="table-card">
          <h3>Consumers</h3>
          <table>
            <thead><tr><th>Name</th><th>Filter</th><th>Ack</th><th>Pending</th><th>Ack Pending</th><th>Redelivered</th><th>Delivered</th></tr></thead>
            <tbody>
              <tr v-for="c in state.streamDetail?.consumers || []" :key="c.name" @click="openConsumer(c)">
                <td class="link">{{ c.name }}</td><td>{{ c.filterSubject }}</td><td>{{ c.ackPolicy }}</td><td>{{ c.numPending }}</td><td>{{ c.numAckPending }}</td><td>{{ c.numRedelivered }}</td><td>{{ c.deliveredStream }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section v-if="state.page === 'consumers'" class="page">
        <div class="hero-grid compact">
          <article class="metric"><span>Consumers</span><strong>{{ state.consumers.length }}</strong><em>sorted by risk</em></article>
          <article class="metric"><span>Pending</span><strong>{{ risk.pending }}</strong><em>not delivered</em></article>
          <article class="metric warn"><span>Ack Pending</span><strong>{{ risk.ack }}</strong><em>waiting ack</em></article>
          <article class="metric hot"><span>Redelivered</span><strong>{{ risk.redelivered }}</strong><em>retry pressure</em></article>
        </div>
        <article class="panel"><h3>Pressure Map</h3><div ref="consumerChart" class="chart tall"></div></article>
        <div class="table-card">
          <table>
            <thead><tr><th>Stream</th><th>Name</th><th>Filter</th><th>Pending</th><th>Ack Pending</th><th>Redelivered</th><th>Ack Floor</th><th>Delivered</th></tr></thead>
            <tbody>
              <tr v-for="c in state.consumers" :key="c.streamName + c.name" @click="openConsumer(c)">
                <td>{{ c.streamName }}</td><td class="link">{{ c.name }}</td><td>{{ c.filterSubject }}</td><td>{{ c.numPending }}</td><td>{{ c.numAckPending }}</td><td>{{ c.numRedelivered }}</td><td>{{ c.ackFloorStream }}</td><td>{{ c.deliveredStream }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section v-if="state.page === 'messages'" class="page">
        <div class="toolbar wrap">
          <select v-model="state.selectedStream" @change="scanMessages">
            <option v-for="s in state.streams" :key="s.name" :value="s">{{ s.name }}</option>
          </select>
          <input v-model="state.filters.subjectContains" placeholder="Subject contains" />
          <input v-model="state.filters.payloadContains" placeholder="Payload contains" />
          <input v-model.number="state.filters.limit" placeholder="Limit" />
          <input v-model.number="state.filters.maxProbes" placeholder="Max probes" />
          <button @click="scanMessages">Scan</button>
        </div>
        <div class="scan-strip" v-if="state.scanning || state.scanProgress.scanned">
          <strong>{{ state.scanning ? 'Scanning' : 'Scan complete' }}</strong>
          <span>matched {{ state.scanProgress.matched }}</span>
          <span>scanned {{ state.scanProgress.scanned }}</span>
          <span>current seq {{ state.scanProgress.current }}</span>
        </div>
        <div class="table-card split">
          <table>
            <thead><tr><th>Seq</th><th>Scheduled</th><th>Received</th><th>Shard</th><th>Queue</th><th>Job</th><th>Job ID</th><th>Payload</th></tr></thead>
            <tbody>
              <tr v-for="m in state.messages" :key="m.sequence" @click="state.selectedMessage = m">
                <td>{{ m.sequence }}</td><td>{{ fmtTime(m.scheduledAt) }}</td><td>{{ fmtTime(m.time) }}</td><td>{{ m.shard }}</td><td>{{ m.queue }}</td><td>{{ m.job }}</td><td>{{ m.jobId }}</td><td>{{ m.data }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </section>

      <section v-if="state.page === 'cli'" class="page cli-page">
        <article class="panel cli-console">
          <div class="cli-head">
            <div>
              <p class="eyebrow">NATS command bridge</p>
              <h3>CLI</h3>
              <p class="muted">Runs the installed <code>nats</code> binary with parsed arguments. This does not invoke PowerShell, cmd, bash, or arbitrary shell commands.</p>
            </div>
            <div class="cli-flags">
              <label><input type="checkbox" v-model="state.cli.useConnection" /> use current connection</label>
              <label>timeout <input type="number" min="1" max="300" v-model.number="state.cli.timeoutSeconds" /></label>
            </div>
          </div>
          <div class="cli-input">
            <span class="prompt">nats</span>
            <textarea v-model="state.cli.command" @keydown.ctrl.enter.prevent="runCLICommand" placeholder="stream ls"></textarea>
            <button class="primary" :disabled="state.cli.running" @click="runCLICommand">{{ state.cli.running ? 'Running...' : 'Run' }}</button>
          </div>
          <div class="quick-row">
            <button @click="useCLIExample('server info')">server info</button>
            <button @click="useCLIExample('stream ls')">stream ls</button>
            <button @click="useCLIExample('consumer ls SCHEDULER_0')">consumer ls</button>
            <button @click="useCLIExample('stream info SCHEDULER_0')">stream info</button>
          </div>
        </article>
        <article class="terminal-card">
          <div v-if="!state.cli.results.length" class="terminal-empty">
            <strong>No commands yet</strong>
            <span>Try <code>stream ls</code> or paste any <code>nats</code> command arguments.</span>
          </div>
          <div v-for="(result, i) in state.cli.results" :key="i" class="terminal-run" :class="{ failed: result.exitCode !== 0 }">
            <header>
              <strong>{{ result.command }}</strong>
              <span>exit {{ result.exitCode }} - {{ result.durationMillis }}ms - {{ fmtTime(result.startedAt) }}</span>
            </header>
            <pre v-if="result.stdout">{{ result.stdout }}</pre>
            <pre v-if="result.stderr" class="stderr">{{ result.stderr }}</pre>
          </div>
        </article>
      </section>

      <section v-if="state.page === 'storage'" class="page">
        <div class="chart-grid">
          <article class="panel"><h3>Key-Value Buckets</h3><div class="bucket" v-for="b in state.kv" :key="b.name"><strong>{{ b.name }}</strong><span>{{ b.values }} values · {{ bytes(b.bytes) }}</span></div></article>
          <article class="panel"><h3>Object Stores</h3><div class="bucket" v-for="b in state.objects" :key="b.name"><strong>{{ b.name }}</strong><span>{{ bytes(b.bytes) }} · {{ b.storage }}</span></div></article>
        </div>
      </section>
    </section>

    <aside class="drawer" v-if="state.selectedMessage">
      <button class="ghost" @click="state.selectedMessage = null">Close</button>
      <h2>Message {{ state.selectedMessage.sequence }}</h2>
      <dl>
        <dt>Subject</dt><dd>{{ state.selectedMessage.subject }}</dd>
        <dt>Scheduled</dt><dd>{{ fmtTime(state.selectedMessage.scheduledAt) }}</dd>
        <dt>Received</dt><dd>{{ fmtTime(state.selectedMessage.time) }}</dd>
      </dl>
      <button class="primary" @click="republish(state.selectedMessage)">Republish</button>
      <h3>Headers</h3><pre>{{ JSON.stringify(state.selectedMessage.headers, null, 2) }}</pre>
      <h3>Payload</h3><pre>{{ pretty(state.selectedMessage.data) }}</pre>
    </aside>

    <aside class="drawer" v-if="state.selectedConsumer">
      <button class="ghost" @click="state.selectedConsumer = null">Close</button>
      <h2>{{ state.selectedConsumer.name }}</h2>
      <p class="muted">NATS exposes ack counts plus ack-floor/delivered bounds, not the exact internal ack-pending set. These are candidate stream messages in that window.</p>
      <dl>
        <dt>Stream</dt><dd>{{ state.selectedConsumer.streamName }}</dd>
        <dt>Filter</dt><dd>{{ state.selectedConsumer.filterSubject }}</dd>
        <dt>Ack floor</dt><dd>{{ state.selectedConsumer.ackFloorStream }}</dd>
        <dt>Delivered</dt><dd>{{ state.selectedConsumer.deliveredStream }}</dd>
      </dl>
      <div class="mini-list">
        <button v-for="m in state.candidateMessages" :key="m.sequence" @click="openCandidateMessage(m)">
          #{{ m.sequence }} · {{ fmtTime(m.scheduledAt) }} · {{ m.subject }}
        </button>
      </div>
    </aside>
  </main>
</template>
