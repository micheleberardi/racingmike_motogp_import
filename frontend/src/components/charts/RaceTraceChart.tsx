'use client'

import ReactECharts from 'echarts-for-react'
import type { RaceTracePoint } from '@/types'

const CHART_COLORS = [
  '#E8002D', '#00A8E8', '#FFD700', '#00E496', '#FF6B35',
  '#9B59B6', '#1ABC9C', '#E74C3C', '#3498DB', '#F39C12',
  '#2ECC71', '#E91E63', '#00BCD4', '#FF5722', '#607D8B',
]

interface Props {
  raceTrace: RaceTracePoint[]
  riders: string[]
}

export default function RaceTraceChart({ raceTrace, riders }: Props) {
  const riderColors: Record<string, string> = {}
  riders.forEach((r, i) => {
    riderColors[r] = CHART_COLORS[i % CHART_COLORS.length]
  })

  const series = riders.map((rider) => {
    const data = raceTrace
      .filter((d) => d.rider_name === rider)
      .sort((a, b) => a.lap_number - b.lap_number)
      .map((d) => [d.lap_number, parseFloat(d.gap_s.toFixed(3))])

    return {
      name: rider,
      type: 'line',
      smooth: false,
      data,
      lineStyle: { width: 2, color: riderColors[rider] },
      itemStyle: { color: riderColors[rider] },
      symbol: 'none',
      emphasis: { lineStyle: { width: 3 } },
    }
  })

  const option = {
    backgroundColor: '#111111',
    grid: { top: 40, right: 20, bottom: 80, left: 70 },
    xAxis: {
      type: 'value',
      name: 'Lap',
      nameLocation: 'middle',
      nameGap: 28,
      min: 1,
      minInterval: 1,
      axisLine: { lineStyle: { color: '#333333' } },
      axisLabel: { color: '#888888', fontSize: 11 },
      splitLine: { lineStyle: { color: '#1a1a1a' } },
    },
    yAxis: {
      type: 'value',
      name: 'Gap to leader (s)',
      nameLocation: 'middle',
      nameGap: 55,
      inverse: false,
      axisLine: { lineStyle: { color: '#333333' } },
      axisLabel: {
        color: '#888888',
        fontSize: 11,
        formatter: (v: number) => (v === 0 ? '0' : `+${v.toFixed(1)}`),
      },
      splitLine: { lineStyle: { color: '#1a1a1a' } },
    },
    series,
    legend: {
      type: 'scroll',
      bottom: 0,
      textStyle: { color: '#888888', fontSize: 11 },
      pageTextStyle: { color: '#888888' },
      pageIconColor: '#E8002D',
      pageIconInactiveColor: '#333333',
    },
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#1a1a1a',
      borderColor: '#333333',
      textStyle: { color: '#ffffff', fontSize: 12 },
      formatter: (params: { seriesName: string; data: [number, number] }[]) => {
        if (!params.length) return ''
        const lap = params[0].data[0]
        let html = `<div style="font-weight:bold;margin-bottom:4px;color:#888">Lap ${lap}</div>`
        const sorted = [...params].sort((a, b) => a.data[1] - b.data[1])
        for (const p of sorted) {
          const gap = p.data[1]
          const gapStr = gap === 0 ? 'LEADER' : `+${gap.toFixed(3)}s`
          html += `<div style="display:flex;align-items:center;gap:8px;margin:2px 0">
            <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${riderColors[p.seriesName]}"></span>
            <span style="flex:1">${p.seriesName}</span>
            <span style="font-weight:bold;color:${gap === 0 ? '#FFD700' : '#fff'}">${gapStr}</span>
          </div>`
        }
        return html
      },
    },
  }

  return (
    <ReactECharts
      option={option}
      style={{ height: '420px', width: '100%' }}
      opts={{ renderer: 'canvas' }}
    />
  )
}
