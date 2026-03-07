import { useState, useEffect, useRef } from 'react'
import { Send, Mic, MicOff, Trash2, Zap, RefreshCw, ChevronDown } from 'lucide-react'
import api from '../api/client'

// ── Shared constants ──────────────────────────────────────────────────────────

const STATUS_WA = {
  idle:      { label: 'Aguardando',    cls: 'bg-gray-100 text-gray-500' },
  searching: { label: 'Buscando carga', cls: 'bg-amber-100 text-amber-700' },
  matched:   { label: 'Carga aceita',   cls: 'bg-green-100 text-green-700' },
  in_trip:   { label: 'Em viagem',      cls: 'bg-blue-100 text-blue-700' },
  delivered: { label: 'Entregue',       cls: 'bg-purple-100 text-purple-700' },
}

const STATUS_OPS = {
  idle:      { label: '💤 Aguardando',    border: 'border-gray-700',  badge: 'bg-gray-800 text-gray-400' },
  searching: { label: '🔍 Buscando',      border: 'border-amber-500', badge: 'bg-amber-950 text-amber-400' },
  matched:   { label: '✅ Carga aceita',  border: 'border-green-500', badge: 'bg-green-950 text-green-400' },
  in_trip:   { label: '🚛 Em viagem',     border: 'border-blue-500',  badge: 'bg-blue-950 text-blue-400' },
  delivered: { label: '🏁 Entregue',      border: 'border-purple-500', badge: 'bg-purple-950 text-purple-400' },
}

const SUGGESTIONS = [
  { short: 'Buscar carga SP→TO', full: 'Olá! Estou em São Paulo e quero ir para Tocantins, tem carga disponível para minha carreta?' },
  { short: 'Aceitar carga nº 1', full: 'Quero a carga número 1' },
  { short: 'Iniciei o trajeto',  full: 'Saí para fazer a coleta, iniciei o trajeto agora' },
  { short: 'Entrega concluída',  full: 'Cheguei no destino, entrega concluída!' },
]

function getInitials(name = '') {
  return name.split(' ').slice(0, 2).map(p => p[0]).join('').toUpperCase()
}

// ── Left panel: WhatsApp simulator ───────────────────────────────────────────

function WhatsAppPanel({ drivers, convs, setConvs }) {
  const [selectedId, setSelectedId] = useState(null)
  const [message, setMessage]       = useState('')
  const [loading, setLoading]       = useState(false)
  const [recording, setRecording]   = useState(false)
  const [audioBlob, setAudioBlob]   = useState(null)
  const mediaRecorderRef   = useRef(null)
  const chunksRef          = useRef([])
  const messagesEndRef     = useRef(null)
  const scrollContainerRef = useRef(null)
  const isAtBottomRef      = useRef(true)
  const prevSelectedIdRef  = useRef(null)
  const [showScrollBtn, setShowScrollBtn] = useState(false)

  useEffect(() => {
    if (!selectedId && drivers.length > 0) setSelectedId(drivers[0].id)
  }, [drivers, selectedId])

  const handleScroll = () => {
    const el = scrollContainerRef.current
    if (!el) return
    const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 80
    isAtBottomRef.current = atBottom
    setShowScrollBtn(!atBottom)
  }

  const scrollToBottom = (behavior = 'smooth') => {
    messagesEndRef.current?.scrollIntoView({ behavior })
    isAtBottomRef.current = true
    setShowScrollBtn(false)
  }

  useEffect(() => {
    const switched = prevSelectedIdRef.current !== selectedId
    prevSelectedIdRef.current = selectedId
    if (switched) {
      scrollToBottom('instant')
    } else if (isAtBottomRef.current) {
      scrollToBottom('smooth')
    }
  }, [convs, selectedId])

  const driver = drivers.find(d => d.id === selectedId)
  const conv   = selectedId ? (convs[selectedId] || { messages: [], status: 'idle', context: {} }) : null

  const sendText = async (text = message) => {
    if (!text.trim() || !selectedId || loading) return
    setLoading(true)
    scrollToBottom()
    try {
      const r = await api.post('/message', { driver_id: selectedId, message: text.trim() })
      setConvs(prev => ({ ...prev, [selectedId]: r.data.state }))
      setMessage('')
    } catch (e) {
      alert(e.response?.data?.detail || 'Erro ao processar mensagem.')
    } finally {
      setLoading(false)
    }
  }

  const startRecording = async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true })
      chunksRef.current = []
      const mr = new MediaRecorder(stream)
      mr.ondataavailable = e => chunksRef.current.push(e.data)
      mr.onstop = () => {
        setAudioBlob(new Blob(chunksRef.current, { type: 'audio/webm' }))
        stream.getTracks().forEach(t => t.stop())
      }
      mr.start()
      mediaRecorderRef.current = mr
      setRecording(true)
    } catch { alert('Permissão de microfone negada.') }
  }

  const stopRecording = () => { mediaRecorderRef.current?.stop(); setRecording(false) }

  const sendAudio = async () => {
    if (!audioBlob || !selectedId || loading) return
    setLoading(true)
    const form = new FormData()
    form.append('file', audioBlob, 'audio.webm')
    try {
      const r = await api.post(`/audio/${selectedId}`, form, { headers: { 'Content-Type': 'multipart/form-data' } })
      setConvs(prev => ({ ...prev, [selectedId]: r.data.state }))
      setAudioBlob(null)
    } catch (e) {
      alert(e.response?.data?.detail || 'Erro na transcrição de áudio.')
    } finally {
      setLoading(false)
    }
  }

  const resetConv = async (id) => {
    await api.delete(`/state/${id}`)
    setConvs(prev => ({ ...prev, [id]: { messages: [], status: 'idle', context: {} } }))
  }

  return (
    <div className="flex h-full" style={{ background: '#e5ddd5' }}>

      {/* Contacts sidebar */}
      <div className="w-52 min-w-[180px] bg-white flex flex-col border-r border-gray-200 flex-shrink-0">
        <div className="bg-motz px-3 py-2.5 flex items-center justify-between">
          <span className="text-white font-semibold text-xs tracking-wide">Motoristas</span>
          <button
            onClick={async () => { await api.delete('/state'); setConvs({}) }}
            className="text-orange-200 hover:text-white text-[10px] transition-colors"
          >
            Limpar
          </button>
        </div>
        <div className="flex-1 overflow-y-auto scrollbar-thin">
          {drivers.map(d => {
            const s    = convs[d.id]?.status || 'idle'
            const cfg  = STATUS_WA[s] || STATUS_WA.idle
            const last = convs[d.id]?.messages?.slice(-1)[0]?.content || ''
            const active = d.id === selectedId
            return (
              <button
                key={d.id}
                onClick={() => setSelectedId(d.id)}
                className={`w-full text-left flex items-center gap-2 px-3 py-2.5 border-b border-gray-50 transition-colors ${active ? 'bg-motz-bg' : 'hover:bg-gray-50'}`}
              >
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-white font-bold text-[11px] flex-shrink-0 ${active ? 'bg-motz' : 'bg-gray-300'}`}>
                  {getInitials(d.nome)}
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-baseline gap-1 min-w-0">
                    <span className="font-semibold text-[11px] text-gray-900 truncate">{d.nome.split(' ')[0]}</span>
                    <span className="text-[9px] text-gray-400 flex-shrink-0">{d.localizacao_estado}</span>
                  </div>
                  <span className={`inline-block text-[9px] px-1.5 py-0.5 rounded-full font-semibold ${cfg.cls}`}>{cfg.label}</span>
                  {last && <p className="text-[9px] text-gray-400 truncate mt-0.5">{last}</p>}
                </div>
              </button>
            )
          })}
        </div>
      </div>

      {/* Chat area */}
      {driver && conv ? (
        <div className="flex-1 flex flex-col min-w-0 relative">

          {/* chat header */}
          <div className="bg-motz px-4 py-2.5 flex items-center gap-2.5 shadow-sm flex-shrink-0">
            <div className="w-8 h-8 rounded-full bg-orange-400 flex items-center justify-center text-white font-bold text-[11px] flex-shrink-0">
              {getInitials(driver.nome)}
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-white font-semibold text-sm">{driver.nome}</div>
              <div className="text-orange-100 text-[10px] truncate">
                {driver.veiculo.composicao} {driver.veiculo.caracteristica} · {driver.localizacao_atual}/{driver.localizacao_estado}
              </div>
            </div>
            <button onClick={() => resetConv(driver.id)} className="text-orange-200 hover:text-white transition-colors" title="Limpar conversa">
              <Trash2 size={14} />
            </button>
          </div>

          {/* accepted load banner */}
          {conv.context?.carga_aceita && (
            <div className="bg-green-50 border-b border-green-200 px-4 py-1.5 flex items-center gap-2 text-xs text-green-800 flex-shrink-0">
              <span className="font-semibold">Carga aceita:</span>
              <span className="truncate">
                {conv.context.carga_aceita.tipo_carga} ·{' '}
                {conv.context.carga_aceita.origem_estado} → {conv.context.carga_aceita.destino_estado} ·{' '}
                <strong>R$ {conv.context.carga_aceita.valor_frete?.toLocaleString('pt-BR')}</strong>
              </span>
            </div>
          )}

          {/* messages */}
          <div
            ref={scrollContainerRef}
            onScroll={handleScroll}
            className="flex-1 overflow-y-auto px-4 py-3 flex flex-col gap-1.5 scrollbar-thin"
            style={{ backgroundImage: "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='400' height='400'%3E%3Crect fill='%23ddd6cb' width='400' height='400'/%3E%3C/svg%3E\")" }}
          >
            {conv.messages?.length === 0 && (
              <div className="flex flex-col items-center justify-center h-full text-gray-400 gap-2">
                <span className="text-2xl">💬</span>
                <p className="text-xs text-gray-500">Use as sugestões abaixo para iniciar</p>
              </div>
            )}
            {conv.messages?.map((msg, i) => {
              const isDriver = msg.role === 'driver'
              return (
                <div key={i} className={`flex ${isDriver ? 'justify-end' : 'justify-start'}`}>
                  <div className={`max-w-[80%] px-3 py-2 rounded-2xl text-xs shadow-sm ${isDriver ? 'bg-motz-bg-dark text-gray-800 rounded-br-sm' : 'bg-white text-gray-800 rounded-bl-sm'}`}>
                    <p className="whitespace-pre-wrap leading-relaxed">{msg.content}</p>
                    <p className={`text-[9px] mt-0.5 text-right ${isDriver ? 'text-orange-400' : 'text-gray-300'}`}>{msg.timestamp}</p>
                  </div>
                </div>
              )
            })}
            {loading && (
              <div className="flex justify-start">
                <div className="bg-white rounded-2xl rounded-bl-sm px-3 py-2.5 shadow-sm">
                  <div className="flex gap-1 items-center">
                    {[0, 150, 300].map(d => (
                      <span key={d} className="w-1.5 h-1.5 bg-gray-300 rounded-full animate-bounce" style={{ animationDelay: `${d}ms` }} />
                    ))}
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>

          {/* scroll-to-bottom button */}
          {showScrollBtn && (
            <button
              onClick={() => scrollToBottom()}
              className="absolute bottom-20 right-4 w-8 h-8 bg-white rounded-full shadow-lg border border-gray-200 flex items-center justify-center text-gray-500 hover:text-motz hover:border-motz transition-all z-10"
              title="Ir para as mensagens mais recentes"
            >
              <ChevronDown size={15} />
            </button>
          )}

          {/* suggestions */}
          <div className="bg-white border-t border-gray-100 px-3 py-1.5 flex gap-1.5 overflow-x-auto scrollbar-thin flex-shrink-0">
            <span className="flex items-center gap-1 text-[10px] text-gray-400 flex-shrink-0">
              <Zap size={10} className="text-motz" /> Sugestões:
            </span>
            {SUGGESTIONS.map((s, i) => (
              <button
                key={i}
                onClick={() => sendText(s.full)}
                disabled={loading}
                className="flex-shrink-0 text-[10px] bg-motz-bg hover:bg-motz hover:text-white text-motz border border-orange-200 rounded-full px-2.5 py-1 transition-all disabled:opacity-40 font-medium"
              >
                {s.short}
              </button>
            ))}
          </div>

          {/* input */}
          <div className="bg-[#f0f0f0] px-3 py-2 flex items-center gap-2 border-t border-gray-200 flex-shrink-0">
            {audioBlob ? (
              <div className="flex items-center gap-2 flex-1 bg-white rounded-full px-3 py-1.5 text-xs">
                <span className="text-motz text-[10px] font-medium">Áudio gravado — pronto para enviar</span>
                <button onClick={() => setAudioBlob(null)} className="ml-auto text-gray-400 hover:text-gray-600 text-xs">✕</button>
              </div>
            ) : (
              <>
                <button
                  onMouseDown={startRecording} onMouseUp={stopRecording}
                  onTouchStart={startRecording} onTouchEnd={stopRecording}
                  className={`w-8 h-8 rounded-full flex items-center justify-center transition-all flex-shrink-0 ${recording ? 'bg-red-500 scale-110' : 'bg-gray-200 hover:bg-gray-300 text-gray-600'}`}
                >
                  {recording ? <MicOff size={14} className="text-white" /> : <Mic size={14} />}
                </button>
                <input
                  type="text"
                  value={message}
                  onChange={e => setMessage(e.target.value)}
                  onKeyDown={e => e.key === 'Enter' && !e.shiftKey && sendText()}
                  placeholder="Mensagem do motorista..."
                  className="flex-1 bg-white rounded-full px-3 py-2 text-xs outline-none focus:ring-2 focus:ring-motz/30"
                  disabled={loading}
                />
              </>
            )}
            <button
              onClick={audioBlob ? sendAudio : () => sendText()}
              disabled={loading || (!message.trim() && !audioBlob)}
              className="w-8 h-8 bg-motz hover:bg-motz-dark disabled:bg-gray-300 rounded-full flex items-center justify-center text-white transition-all flex-shrink-0"
            >
              {loading
                ? <div className="w-3 h-3 border-2 border-white/40 border-t-white rounded-full animate-spin" />
                : <Send size={13} />}
            </button>
          </div>
        </div>
      ) : (
        <div className="flex-1 flex items-center justify-center text-gray-400 text-xs">
          Selecione um motorista
        </div>
      )}
    </div>
  )
}

// ── Right panel: Live operations ──────────────────────────────────────────────

function OpsPanel({ drivers, convs, lastUpdated, autoRefresh, setAutoRefresh }) {
  const convList = Object.values(convs)
  const kpis = {
    ativas:   convList.filter(c => c.status !== 'idle').length,
    buscando: convList.filter(c => c.status === 'searching').length,
    aceitas:  convList.filter(c => c.status === 'matched').length,
    emViagem: convList.filter(c => c.status === 'in_trip').length,
    valor:    convList
      .filter(c => ['matched', 'in_trip', 'delivered'].includes(c.status))
      .reduce((acc, c) => acc + (c.context?.carga_aceita?.valor_frete || 0), 0),
  }

  const feed = drivers
    .flatMap(d => (convs[d.id]?.messages || []).map(m => ({ ...m, driverName: d.nome.split(' ')[0] })))
    .slice(-30).reverse()

  return (
    <div className="h-full bg-gray-950 text-white flex flex-col overflow-hidden">

      {/* Header */}
      <div className="flex items-center justify-between px-5 pt-4 pb-3 flex-shrink-0">
        <div className="flex items-center gap-2">
          <span className="relative flex h-2.5 w-2.5">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
            <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-green-500" />
          </span>
          <h2 className="text-base font-extrabold text-white tracking-tight">Central de Operações</h2>
          {lastUpdated && <span className="text-[10px] text-gray-600 ml-1">· {lastUpdated}</span>}
        </div>
        <label className="flex items-center gap-1.5 text-[11px] text-gray-500 cursor-pointer select-none">
          <input type="checkbox" checked={autoRefresh} onChange={e => setAutoRefresh(e.target.checked)} className="accent-motz" />
          <RefreshCw size={11} className={autoRefresh ? 'text-green-400 animate-spin' : 'text-gray-600'} style={{ animationDuration: '3s' }} />
        </label>
      </div>

      {/* KPIs */}
      <div className="grid grid-cols-5 gap-2 px-5 mb-4 flex-shrink-0">
        {[
          { value: kpis.ativas,  label: 'Ativas',    color: 'text-motz' },
          { value: kpis.buscando, label: 'Buscando',  color: 'text-amber-400' },
          { value: kpis.aceitas,  label: 'Aceitas',   color: 'text-green-400' },
          { value: kpis.emViagem, label: 'Em viagem', color: 'text-blue-400' },
          { value: `R$${(kpis.valor/1000).toFixed(0)}k`, label: 'Negociado', color: 'text-motz' },
        ].map(k => (
          <div key={k.label} className="bg-gray-900 rounded-xl px-3 py-2.5 border border-gray-800">
            <div className={`text-xl font-extrabold ${k.color} tabular-nums`}>{k.value}</div>
            <div className="text-gray-500 text-[9px] uppercase tracking-widest mt-0.5">{k.label}</div>
          </div>
        ))}
      </div>

      {/* Body: driver cards + activity feed */}
      <div className="flex flex-1 gap-4 px-5 pb-5 min-h-0">

        {/* Driver cards */}
        <div className="flex-1 overflow-y-auto scrollbar-thin min-w-0 space-y-2">
          <h3 className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2">Motoristas</h3>
          {drivers.map(d => {
            const s   = convs[d.id]?.status || 'idle'
            const cfg = STATUS_OPS[s] || STATUS_OPS.idle
            const ctx = convs[d.id]?.context || {}
            const carga = ctx.carga_aceita
            const lastMsg = convs[d.id]?.messages?.slice(-1)[0]?.content
            return (
              <div key={d.id} className={`bg-gray-900 rounded-xl p-3 border-l-2 ${cfg.border} border border-gray-800 transition-all`}>
                <div className="flex items-center gap-2">
                  <div className="w-8 h-8 rounded-full bg-gray-800 border border-gray-700 flex items-center justify-center text-motz font-bold text-[10px] flex-shrink-0">
                    {getInitials(d.nome)}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      <span className="font-semibold text-white text-xs">{d.nome}</span>
                      <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-bold ${cfg.badge}`}>{cfg.label}</span>
                    </div>
                    <p className="text-gray-600 text-[9px] mt-0.5 truncate">
                      {d.veiculo.composicao} {d.veiculo.caracteristica} · {d.localizacao_atual}/{d.localizacao_estado}
                    </p>
                  </div>
                </div>
                {carga && (
                  <div className="mt-2 bg-gray-800 rounded-lg px-2.5 py-1.5 text-[10px] text-gray-300 flex flex-wrap gap-x-2 gap-y-0.5">
                    <span className="font-medium text-white">{carga.tipo_carga}</span>
                    <span>{carga.origem_estado} → {carga.destino_estado}</span>
                    <span className="text-motz font-semibold">R$ {carga.valor_frete?.toLocaleString('pt-BR')}</span>
                  </div>
                )}
                {lastMsg && !carga && (
                  <p className="mt-1 text-[9px] text-gray-700 italic truncate">"{lastMsg}"</p>
                )}
              </div>
            )
          })}
        </div>

        {/* Activity feed */}
        <div className="w-52 flex-shrink-0 flex flex-col min-h-0">
          <h3 className="text-[10px] font-bold text-gray-500 uppercase tracking-widest mb-2">Feed ao vivo</h3>
          <div className="flex-1 bg-gray-900 rounded-xl border border-gray-800 overflow-y-auto scrollbar-thin divide-y divide-gray-800">
            {feed.length === 0 && (
              <div className="text-center text-gray-700 py-8 text-[10px] px-3">
                Nenhuma atividade ainda.<br />
                <span className="text-gray-800">Inicie uma conversa ao lado.</span>
              </div>
            )}
            {feed.map((msg, i) => (
              <div key={i} className="px-3 py-2 hover:bg-gray-800/50 transition-colors">
                <div className="flex items-center gap-1.5 mb-0.5">
                  <span className="text-motz text-[9px] font-bold">{msg.timestamp}</span>
                  <span className="text-gray-600 text-[9px]">{msg.role === 'driver' ? '👤' : '🤖'} {msg.driverName}</span>
                </div>
                <p className="text-gray-400 text-[9px] leading-relaxed line-clamp-2">{msg.content}</p>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

// ── Main Demo page ────────────────────────────────────────────────────────────

export default function Demo() {
  const [drivers, setDrivers]         = useState([])
  const [convs, setConvs]             = useState({})
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [lastUpdated, setLastUpdated] = useState(null)

  useEffect(() => {
    api.get('/drivers').then(r => setDrivers(r.data))
  }, [])

  useEffect(() => {
    const load = () =>
      api.get('/state').then(r => {
        setConvs(r.data.conversations || {})
        setLastUpdated(new Date().toLocaleTimeString('pt-BR'))
      })
    load()
    if (!autoRefresh) return
    const id = setInterval(load, 3000)
    return () => clearInterval(id)
  }, [autoRefresh])

  return (
    <div className="flex h-[calc(100vh-64px)] overflow-hidden">
      {/* Left: WhatsApp */}
      <div className="w-1/2 border-r border-gray-300 flex flex-col overflow-hidden flex-shrink-0">
        <WhatsAppPanel drivers={drivers} convs={convs} setConvs={setConvs} />
      </div>

      {/* Right: Ops Center */}
      <div className="w-1/2 overflow-hidden flex-shrink-0">
        <OpsPanel
          drivers={drivers}
          convs={convs}
          lastUpdated={lastUpdated}
          autoRefresh={autoRefresh}
          setAutoRefresh={setAutoRefresh}
        />
      </div>
    </div>
  )
}
