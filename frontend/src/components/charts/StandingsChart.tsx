'use client'

import ReactECharts from 'echarts-for-react'
import type { Standing } from '@/types'

interface Props {
  standings: Standing[]
  limit?: number
}

export default function StandingsChart({ standings, limit = 10 }: Props) {
  const top = standings.slice(0, limit)

  const option = {
    backgroundColor: '#111111',
    grid: { top: 10, right: 80, bottom: 10, left: 160, containLabel: false },
    xAxis: {
      type: 'value',
      axisLabel: { color: '#888888', fontSize: 11 },
      axisLine: { lineStyle: { color: '#333333' } },
      splitLine: { lineStyle: { color: '#1a1a1a' } },
    },
    yAxis: {
      type: 'category',
      data: top.map((s) => s.rider_full_name),
      inverse: true,
      axisLabel: { color: '#cccccc', fontSize: 11 },
      axisLine: { lineStyle: { color: '#333333' } },
    },
    series: [
      {
        type: 'bar',
        data: top.map((s, i) => ({
          value: s.points,
          itemStyle: {
            color:
              i === 0
                ? '#FFD700'
                : i === 1
                ? '#C0C0C0'
                : i === 2
                ? '#CD7F32'
                : s.team_color || '#E8002D',
            opacity: i < 3 ? 1 : 0.7,
          },
        })),
        label: {
          show: true,
          position: 'right',
          color: '#cccccc',
          fontSize: 12,
          fontWeight: 'bold',
        },
        barMaxWidth: 28,
      },
    ],
    tooltip: {
      trigger: 'axis',
      backgroundColor: '#1a1a1a',
      borderColor: '#333333',
      textStyle: { color: '#fff', fontSize: 12 },
    },
  }

  return (
    <ReactECharts
      option={option}
      style={{ height: `${Math.max(200, top.length * 32 + 40)}px`, width: '100%' }}
      opts={{ renderer: 'canvas' }}
    />
  )
}
