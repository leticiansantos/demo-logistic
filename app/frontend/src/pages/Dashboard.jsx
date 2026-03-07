import { useState, useEffect } from 'react'
import { ExternalLink, RefreshCw, BarChart2, TrendingUp, Package, Truck, Users } from 'lucide-react'
import {
  PieChart, Pie, Cell,
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line,
} from 'recharts'
import api from '../api/client'

const MOTZ  = '#FF5402'
const LIGHT = '#FF7A35'
const MUTED = '#BDBDBD'
const BLUE  = '#1E88E5'
const PIE_COLORS = [MOTZ, LIGHT, MUTED, '#42A5F5', '#AB47BC']

const DATABRICKS_URL =
  'https://fevm-leticia-santos-classic-stable.cloud.databricks.com/dashboardsv3/01f11987b498173aba3688e58d455efd/published?o=7474658265676932'

const tooltipStyle = { backgroundColor: '#fff', border: '1px solid #f0f0f0', borderRadius: 8, fontSize: 12 }

function fmtBRL(v) {
  return `R$ ${Number(v || 0).toLocaleString('pt-BR', { maximumFractionDigits: 0 })}`
}
function fmtNum(v) {
  return Number(v || 0).toLocaleString('pt-BR')
}

function KpiCard({ icon: Icon, label, value, sub, accent = false }) {
  return (
    <div className={`bg-white rounded-2xl p-4 shadow-sm border border-gray-100 ${accent ? 'border-l-4 border-l-motz' : ''}`}>
      <div className="flex items-start justify-between">
        <div>
          <p className="text-[10px] text-gray-400 uppercase tracking-widest font-semibold">{label}</p>
          <p className="text-2xl font-extrabold text-gray-900 mt-1 tabular-nums">{value}</p>
          {sub && <p className="text-xs text-gray-400 mt-0.5">{sub}</p>}
        </div>
        {Icon && (
          <div className="w-9 h-9 rounded-xl bg-motz-bg flex items-center justify-center flex-shrink-0">
            <Icon size={16} className="text-motz" />
          </div>
        )}
      </div>
    </div>
  )
}

function ChartCard({ title, children, className = '' }) {
  return (
    <div className={`bg-white rounded-2xl p-5 shadow-sm border border-gray-100 ${className}`}>
      <h3 className="font-semibold text-gray-700 text-sm mb-4">{title}</h3>
      {children}
    </div>
  )
}

export default function Dashboard() {
  const [data, setData]       = useState(null)
  const [error, setError]     = useState(null)
  const [loading, setLoading] = useState(true)

  const fetchData = () => {
    setLoading(true)
    setError(null)
    api.get('/metrics')
      .then(r => { setData(r.data); setLoading(false) })
      .catch(e => { setError(e.response?.data?.detail || 'Erro ao buscar métricas.'); setLoading(false) })
  }

  useEffect(() => { fetchData() }, [])

  if (loading) return (
    <div className="flex flex-col items-center justify-center h-[calc(100vh-64px)] gap-3 text-gray-400">
      <div className="w-8 h-8 border-2 border-motz/30 border-t-motz rounded-full animate-spin" />
      <span className="text-sm">Buscando dados no Databricks...</span>
    </div>
  )

  if (error) return (
    <div className="flex flex-col items-center justify-center h-[calc(100vh-64px)] gap-4 text-gray-400">
      <div className="w-16 h-16 rounded-full bg-orange-50 flex items-center justify-center text-3xl">📊</div>
      <p className="font-semibold text-gray-600">{error}</p>
      <button onClick={fetchData} className="text-sm text-motz hover:underline">Tentar novamente</button>
    </div>
  )

  const { resumo, cargas_por_tipo, cargas_por_composicao, cargas_por_uf_origem,
          cargas_por_uf_destino, cargas_por_mes, ticket_medio, data_quality } = data

  const statusData = [
    { name: 'Realizadas',  value: resumo.cargas_realizadas },
    { name: 'Disponíveis', value: resumo.cargas_disponiveis },
    { name: 'Futuras',     value: resumo.cargas_futuras },
  ]

  const dqTotal = (data_quality || []).reduce((s, r) => s + r.registros_com_problema, 0)

  return (
    <div className="max-w-screen-xl mx-auto px-6 py-6">

      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-extrabold text-gray-900 tracking-tight">Visão de Negócio</h1>
          <p className="text-gray-400 text-sm mt-0.5">Dados ao vivo do Databricks · {new Date().toLocaleDateString('pt-BR', { dateStyle: 'long' })}</p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={fetchData}
            className="flex items-center gap-1.5 text-sm text-gray-500 hover:text-motz transition-colors px-3 py-2 rounded-xl hover:bg-motz-bg border border-gray-200"
          >
            <RefreshCw size={14} /> Atualizar
          </button>
          <a
            href={DATABRICKS_URL}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-1.5 text-sm text-white bg-motz hover:bg-motz-dark transition-colors px-3 py-2 rounded-xl shadow-sm"
          >
            <ExternalLink size={14} /> Ver no Databricks
          </a>
        </div>
      </div>

      {/* KPIs row 1 */}
      <div className="grid grid-cols-4 gap-4 mb-4">
        <KpiCard icon={Truck}   label="Transportadoras" value={fmtNum(resumo.total_transportadoras)} accent />
        <KpiCard icon={Users}   label="Motoristas"      value={fmtNum(resumo.total_motoristas)} accent />
        <KpiCard icon={Package} label="Embarcadores"    value={fmtNum(resumo.total_embarcadores)} accent />
        <KpiCard icon={BarChart2} label="Total de cargas" value={fmtNum(resumo.total_cargas)} accent />
      </div>

      {/* KPIs row 2 */}
      <div className="grid grid-cols-4 gap-4 mb-8">
        <KpiCard label="Cargas realizadas"  value={fmtNum(resumo.cargas_realizadas)}  sub="já entregues" />
        <KpiCard label="Cargas disponíveis" value={fmtNum(resumo.cargas_disponiveis)} sub="em aberto" />
        <KpiCard label="Fretes realizados"  value={fmtBRL(resumo.valor_total_fretes_realizados)}
                 icon={TrendingUp} accent sub="valor total" />
        <KpiCard label="Ticket médio"       value={fmtBRL(resumo.valor_medio_frete_realizado)} sub="por frete" />
      </div>

      {/* Charts row 1 */}
      <div className="grid grid-cols-2 gap-5 mb-5">
        <ChartCard title="Cargas por status">
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie data={statusData} dataKey="value" nameKey="name"
                   cx="50%" cy="50%" outerRadius={85} innerRadius={45}
                   label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                   labelLine={false}>
                {statusData.map((_, i) => <Cell key={i} fill={PIE_COLORS[i]} />)}
              </Pie>
              <Tooltip contentStyle={tooltipStyle} formatter={v => [fmtNum(v), 'Cargas']} />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Top 10 tipos de carga">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={cargas_por_tipo} margin={{ left: -10, bottom: 50 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f5f5f5" />
              <XAxis dataKey="tipo_carga" tick={{ fontSize: 11 }} angle={-35} textAnchor="end" interval={0} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="quantidade" name="Cargas" fill={MOTZ} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Charts row 2 */}
      <div className="grid grid-cols-2 gap-5 mb-5">
        <ChartCard title="Composição de veículo">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={cargas_por_composicao} layout="vertical" margin={{ left: 80, right: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f5f5f5" horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis type="category" dataKey="composicao_veiculo" tick={{ fontSize: 11 }} width={80} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="quantidade" name="Cargas" fill={BLUE} radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Cargas realizadas por mês">
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={cargas_por_mes} margin={{ right: 10, bottom: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f5f5f5" />
              <XAxis dataKey="ano_mes" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip contentStyle={tooltipStyle} formatter={v => [fmtNum(v), 'Realizadas']} />
              <Line type="monotone" dataKey="realizadas" name="Realizadas"
                    stroke={MOTZ} strokeWidth={2.5} dot={{ fill: MOTZ, r: 4 }} activeDot={{ r: 6 }} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Charts row 3: UF */}
      <div className="grid grid-cols-2 gap-5 mb-5">
        <ChartCard title="Cargas por UF de origem">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={cargas_por_uf_origem} margin={{ bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f5f5f5" />
              <XAxis dataKey="estado" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="quantidade" name="Cargas" fill={MOTZ} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Cargas por UF de destino">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={cargas_por_uf_destino} margin={{ bottom: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f5f5f5" />
              <XAxis dataKey="estado" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip contentStyle={tooltipStyle} />
              <Bar dataKey="quantidade" name="Cargas" fill={LIGHT} radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>

      {/* Tables row */}
      <div className="grid grid-cols-2 gap-5 mb-8">
        {/* Ticket médio */}
        <ChartCard title="Ticket médio por tipo de carga">
          <div className="overflow-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left py-2 px-1 text-gray-400 font-semibold">Tipo</th>
                  <th className="text-right py-2 px-1 text-gray-400 font-semibold">Qtd</th>
                  <th className="text-right py-2 px-1 text-gray-400 font-semibold">Ticket médio</th>
                  <th className="text-right py-2 px-1 text-gray-400 font-semibold">Valor total</th>
                </tr>
              </thead>
              <tbody>
                {(ticket_medio || []).map((r, i) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 px-1 font-medium text-gray-800">{r.tipo_carga}</td>
                    <td className="py-2 px-1 text-right text-gray-600">{fmtNum(r.quantidade)}</td>
                    <td className="py-2 px-1 text-right text-gray-700">{fmtBRL(r.ticket_medio)}</td>
                    <td className="py-2 px-1 text-right font-semibold text-motz">{fmtBRL(r.valor_total)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </ChartCard>

        {/* Data Quality */}
        <ChartCard title={`Data Quality ${dqTotal > 0 ? `· ${fmtNum(dqTotal)} registros com problema` : '· Sem problemas'}`}>
          <div className="overflow-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="text-left py-2 px-1 text-gray-400 font-semibold">Tabela</th>
                  <th className="text-right py-2 px-1 text-gray-400 font-semibold">Registros com problema</th>
                  <th className="text-right py-2 px-1 text-gray-400 font-semibold">Status</th>
                </tr>
              </thead>
              <tbody>
                {(data_quality || []).map((r, i) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 px-1 font-medium text-gray-800">{r.tabela}</td>
                    <td className="py-2 px-1 text-right text-gray-700">{fmtNum(r.registros_com_problema)}</td>
                    <td className="py-2 px-1 text-right">
                      {r.registros_com_problema === 0
                        ? <span className="text-green-600 font-semibold">✓ OK</span>
                        : <span className="text-amber-600 font-semibold">⚠ {r.registros_com_problema}</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="text-[10px] text-gray-300 mt-3">Dados de qualidade inseridos via notebook 02 para demonstração.</p>
        </ChartCard>
      </div>

      {/* Footer link */}
      <div className="flex justify-center pb-4">
        <a
          href={DATABRICKS_URL}
          target="_blank"
          rel="noopener noreferrer"
          className="flex items-center gap-2 text-sm text-gray-400 hover:text-motz transition-colors"
        >
          <ExternalLink size={13} />
          Ver dashboard completo no Databricks AI/BI
        </a>
      </div>
    </div>
  )
}
