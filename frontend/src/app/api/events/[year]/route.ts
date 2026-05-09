import { NextResponse } from 'next/server'
import pool from '@/lib/db'

export async function GET(
  _request: Request,
  { params }: { params: { year: string } }
) {
  try {
    const year = parseInt(params.year)
    const [rows] = await pool.execute(
      `SELECT id, name, sponsored_name, year, date_start, date_end, circuit_name, country_name, test
       FROM events
       WHERE year = ? AND test = 0
       ORDER BY date_start ASC`,
      [year]
    )
    return NextResponse.json(rows)
  } catch (error) {
    console.error('Error fetching events:', error)
    return NextResponse.json({ error: 'Failed to fetch events' }, { status: 500 })
  }
}
