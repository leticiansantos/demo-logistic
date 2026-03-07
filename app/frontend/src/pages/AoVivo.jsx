import { useState, useEffect } from 'react'
import { RefreshCw, Circle } from 'lucide-react'
import api from '../api/client'

const STATUS = {
  idle:      { label: '💤 Aguardando',    border: 'border-gray-700',  badge: 'bg-gray-800 text-gray-400',    dot: 'bg-gray-600' },
  searching: { label: '🔍 Buscando carga', border: 'border-amber-500', badge: 'bg-amber-950 text-amber-400',  dot: 'bg-amber-400' },
  matched:   { label: '✅ Carga aceita',   border: 'border-green-500', badge: 'bg-green-950 text-green-400',  dot: 'bg-green-400' },
  in_trip:   { label: '🚛 Em viagem',      border: 'border-blue-500',  badge: 'bg-blue-950 text-blue-400',   dot: 'bg-blue-400' },
  delivered: { label: '🏁 Entregue',       border: 'border-purple-500', badge: 'bg-purple-950 text-purple-400', dot: 'bg-purple-400' },
}

function KpiCard({ value, label, color = 'text-motz' }) {
  return (
    <div className="bg-gray-900 rounded-2xl p-5 border border-gray-800">
      <div className={`text-3xl font-extrabold ${color} tabular-nums`}>{value}</div>
      <div className="text-gray-500 text-xs uppercase tracking-widest mt-1.5">{label}</div>
    </div>
  )
}

function getInitials(name = '') {
  return name.split(' ').slice(0, 2).map(p => p[0]).join('').toUpperCase()
}

export default function AoVivo() {
  const [drivers, setDrivers]             = useState([])
  const [convs, setConvs]                 = useState({})
  const [autoRefresh, setAutoRefresh]     = useState(true)
  const [lastUpdated, setLastUpdated]     = useState(null)
  const [available, setAvailable]         = useState(null)

  useEffect(() => {
    api.get('/drivers').then(r => setDrivers(r.data))
    api.get('/loads/available').then(r => setAvailable(r.data))
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

  const convList = Object.values(convs)
  const kpis = {
    ativas:    convList.filter(c => c.status !== 'idle').length,
    buscando:  convList.filter(c => c.status === 'searching').length,
    aceitas:   convList.filter(c => c.status === 'matched').length,
    emViagem:  convList.filter(c => c.status === 'in_trip').length,
    valor:     convList
      .filter(c => ['matched', 'in_trip', 'delivered'].includes(c.status))
      .reduce((acc, c) => acc + (c.context?.carga_aceita?.valor_frete || 0), 0),
  }

  // Activity feed: all messages, most recent first
  const feed = drivers
    .flatMap(d => (convs[d.id]?.messages || []).map(m => ({ ...m, driverName: d.nome })))
    .slice(-20)
    .reverse()

  // Active loads table
  const activeLoads = drivers
    .filter(d => ['matched', 'in_trip', 'delivered'].includes(convs[d.id]?.status))
    .map(d => ({ driver: d, conv: convs[d.id] }))

  return (
    <div className="min-h-screen bg-gray-950 text-white px-6 py-6">

      {/* Header */}
      <div className="flex items-center justify-between mb-8">
        <div>
          <div className="flex items-center gap-3 mb-1">
            <span className="relative flex h-3 w-3">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
              <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500" />
            </span>
            <h1 className="text-2xl font-extrabold text-white tracking-tight">Central de Operações</h1>
          </div>
          <p className="text-gray-500 text-sm ml-6">
            Monitoramento em tempo real · atualiza a cada 3s
            {lastUpdated && <span className="ml-2 text-gray-600">· Última atualização: {lastUpdated}</span>}
          </p>
        </div>
        <label className="flex items-center gap-2 text-sm text-gray-400 cursor-pointer select-none">
          <input
            type="checkbox"
            checked={autoRefresh}
            onChange={e => setAutoRefresh(e.target.checked)}
            className="accent-motz"
          />
          <span>Auto-atualizar</span>
          <RefreshCw size={14} className={autoRefresh ? 'text-green-400 animate-spin' : 'text-gray-600'} style={{ animationDuration: '3s' }} />
        </label>
      </div>

      {/* KPIs — conversas */}
      <div className="flex items-center gap-2 mb-3">
        <span className="text-xs font-bold text-gray-500 uppercase tracking-widest">Conversas de hoje</span>
        <span className="text-[11px] bg-gray-800 text-gray-400 px-2.5 py-0.5 rounded-full border border-gray-700 tabular-nums">
          {new Date().toLocaleDateString('pt-BR', { weekday: 'short', day: '2-digit', month: '2-digit', year: 'numeric' })}
        </span>
      </div>
      <div className="grid grid-cols-5 gap-4 mb-6">
        <KpiCard value={kpis.ativas}  label="Conversas ativas" />
        <KpiCard value={kpis.buscando} label="Buscando carga" color="text-amber-400" />
        <KpiCard value={kpis.aceitas}  label="Carga aceita"   color="text-green-400" />
        <KpiCard value={kpis.emViagem} label="Em viagem"      color="text-blue-400" />
        <KpiCard
          value={`R$ ${kpis.valor.toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`}
          label="Fretes negociados"
        />
      </div>

      {/* Cargas disponíveis */}
      {available && (
        <div className="bg-gray-900 border border-gray-800 rounded-2xl px-6 py-4 mb-8">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-xs font-bold text-gray-500 uppercase tracking-widest">
              Cargas disponíveis para embarque
            </h2>
            <span className="text-2xl font-extrabold text-motz tabular-nums">{available.totais.total}</span>
          </div>

          {/* Período */}
          <div className="grid grid-cols-4 gap-3 mb-5">
            {[
              { label: 'Hoje',          value: available.totais.hoje,           color: 'text-red-400',    bar: 'bg-red-500' },
              { label: 'Próximos 3 dias', value: available.totais.proximos_3_dias, color: 'text-amber-400',  bar: 'bg-amber-500' },
              { label: 'Próxima semana', value: available.totais.proxima_semana,  color: 'text-yellow-400', bar: 'bg-yellow-500' },
              { label: 'Próximo mês',   value: available.totais.proximo_mes,    color: 'text-green-400',  bar: 'bg-green-500' },
            ].map(p => {
              const pct = available.totais.total > 0 ? Math.round((p.value / available.totais.total) * 100) : 0
              return (
                <div key={p.label} className="bg-gray-800 rounded-xl px-4 py-3">
                  <div className={`text-2xl font-extrabold tabular-nums ${p.color}`}>{p.value}</div>
                  <div className="text-gray-500 text-[10px] uppercase tracking-widest mt-0.5 mb-2">{p.label}</div>
                  <div className="h-1 bg-gray-700 rounded-full overflow-hidden">
                    <div className={`h-full rounded-full ${p.bar}`} style={{ width: `${pct}%` }} />
                  </div>
                </div>
              )
            })}
          </div>

          {/* Por tipo de carga */}
          <div>
            <p className="text-[10px] text-gray-600 uppercase tracking-widest mb-2">Por tipo</p>
            <div className="flex flex-wrap gap-2">
              {available.por_tipo.map(t => (
                <div key={t.tipo_carga} className="flex items-center gap-2 bg-gray-800 rounded-lg px-3 py-1.5">
                  <span className="text-white text-xs font-medium">{t.tipo_carga}</span>
                  <span className="text-motz text-xs font-bold tabular-nums">{t.quantidade}</span>
                  {t.proxima_coleta && (
                    <span className="text-gray-600 text-[10px]">· {t.proxima_coleta}</span>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-5 gap-6">

        {/* Driver cards — 3 cols */}
        <div className="col-span-3">
          <h2 className="text-xs font-bold text-gray-500 uppercase tracking-widest mb-4">Motoristas ativos hoje</h2>
          <div className="space-y-3">
            {drivers
              .filter(d => (convs[d.id]?.status || 'idle') !== 'idle')
              .map(d => {
              const s   = convs[d.id]?.status || 'idle'
              const cfg = STATUS[s] || STATUS.idle
              const ctx = convs[d.id]?.context || {}
              const carga = ctx.carga_aceita
              const lastMsgObj = convs[d.id]?.messages?.slice(-1)[0]
              const lastMsg = lastMsgObj?.content
              const lastMsgIsAudio = lastMsgObj?.type === 'audio'
              return (
                <div key={d.id} className={`bg-gray-900 rounded-2xl p-4 border-l-4 ${cfg.border} border border-gray-800 transition-all`}>
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-full bg-gray-800 border border-gray-700 flex items-center justify-center text-motz font-bold text-sm flex-shrink-0">
                      {getInitials(d.nome)}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="font-semibold text-white">{d.nome}</span>
                        <span className={`text-[10px] px-2 py-0.5 rounded-full font-bold ${cfg.badge}`}>
                          {cfg.label}
                        </span>
                      </div>
                      <p className="text-gray-500 text-xs mt-0.5">
                        {d.veiculo.composicao} {d.veiculo.caracteristica} · {d.veiculo.modelo} ·{' '}
                        {d.localizacao_atual}/{d.localizacao_estado}
                      </p>
                    </div>
                  </div>

                  {carga && (
                    <div className="mt-3 bg-gray-800 rounded-xl px-3.5 py-2.5 text-xs text-gray-300 flex flex-wrap gap-x-3 gap-y-1">
                      <span className="font-medium text-white">📦 {carga.tipo_carga}</span>
                      <span>{carga.origem_cidade}/{carga.origem_estado} → {carga.destino_cidade}/{carga.destino_estado}</span>
                      <span className="text-motz font-semibold">R$ {carga.valor_frete?.toLocaleString('pt-BR')}</span>
                      <span className="text-gray-500">Coleta: {carga.data_prevista_coleta}</span>
                    </div>
                  )}

                  {lastMsg && !carga && (
                    <p className="mt-2 text-xs text-gray-600 italic truncate">
                      {lastMsgIsAudio && <span className="not-italic text-violet-500 mr-1">🎙️</span>}
                      "{lastMsgIsAudio ? lastMsg?.replace(/^🎙️\s*/, '') : lastMsg}"
                    </p>
                  )}
                </div>
              )
            })}
            {drivers.length === 0 && (
              <p className="text-gray-700 text-sm text-center py-8">Carregando motoristas...</p>
            )}
            {drivers.length > 0 && drivers.every(d => (convs[d.id]?.status || 'idle') === 'idle') && (
              <p className="text-gray-700 text-sm text-center py-8">Nenhum motorista ativo no momento.</p>
            )}
          </div>
        </div>

        {/* Activity feed — 2 cols */}
        <div className="col-span-2">
          <h2 className="text-xs font-bold text-gray-500 uppercase tracking-widest mb-4">Feed de atividade</h2>
          <div className="bg-gray-900 rounded-2xl border border-gray-800 divide-y divide-gray-800 overflow-y-auto max-h-[calc(100vh-280px)] scrollbar-thin">
            {feed.length === 0 && (
              <div className="text-center text-gray-700 py-10 text-sm">
                <p>Nenhuma atividade ainda.</p>
                <p className="text-xs mt-1 text-gray-800">Inicie uma conversa no Simulador WhatsApp.</p>
              </div>
            )}
            {feed.map((msg, i) => (
              <div key={i} className="px-4 py-3 hover:bg-gray-800/50 transition-colors">
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-motz text-xs font-bold">{msg.timestamp}</span>
                  <span className="text-gray-600 text-xs">
                    {msg.role === 'driver' ? '👤' : '🤖'} {msg.driverName}
                  </span>
                  {msg.type === 'audio' && (
                    <span className="text-[10px] bg-violet-900/60 text-violet-300 px-1.5 py-0.5 rounded font-medium">🎙️ áudio</span>
                  )}
                </div>
                <p className="text-gray-400 text-xs leading-relaxed line-clamp-2">
                  {msg.type === 'audio'
                    ? msg.content?.replace(/^🎙️\s*/, '')
                    : msg.content}
                </p>
              </div>
            ))}
          </div>

          {/* Active loads mini-table */}
          {activeLoads.length > 0 && (
            <>
              <h2 className="text-xs font-bold text-gray-500 uppercase tracking-widest mt-6 mb-3">Cargas em andamento</h2>
              <div className="bg-gray-900 rounded-2xl border border-gray-800 overflow-hidden">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-gray-800">
                      <th className="px-4 py-3 text-left text-gray-500 font-semibold">Motorista</th>
                      <th className="px-4 py-3 text-left text-gray-500 font-semibold">Rota</th>
                      <th className="px-4 py-3 text-right text-gray-500 font-semibold">Valor</th>
                    </tr>
                  </thead>
                  <tbody>
                    {activeLoads.map(({ driver: d, conv: c }) => {
                      const carga = c.context?.carga_aceita
                      if (!carga) return null
                      return (
                        <tr key={d.id} className="border-b border-gray-800 last:border-0 hover:bg-gray-800/50">
                          <td className="px-4 py-2.5 text-white font-medium">{d.nome}</td>
                          <td className="px-4 py-2.5 text-gray-400">
                            {carga.origem_estado} → {carga.destino_estado}
                          </td>
                          <td className="px-4 py-2.5 text-motz font-semibold text-right">
                            R$ {carga.valor_frete?.toLocaleString('pt-BR')}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
