export interface Category {
  id: string
  name: string
  year: number
  legacy_id?: string
}

export interface Event {
  id: string
  name: string
  sponsored_name?: string
  year: number
  date_start: string
  date_end: string
  circuit_name: string
  country_name: string
  test: number
  timezone?: string
}

export interface Session {
  id: string
  type: string
  year: number
  event_id: string
  category_id: string
  category_name: string
  event_name: string
  circuit_name: string
  analysis_url?: string
  status: string
  session_datetime?: string
}

export interface Result {
  session_id: string
  rider_id: string
  rider_full_name: string
  rider_number: string
  team_name: string
  constructor_name: string
  position: number | null
  points: number
  total_laps: number
  time: string
  gap_first: string
  top_speed: number
  average_speed: number
  year: number
  event_id: string
  category_id: string
  session_type: string
  team_color?: string
}

export interface Standing {
  year: number
  category_id: string
  position: number
  rider_id: string
  rider_full_name: string
  rider_number: string
  team_name: string
  constructor_name: string
  points: number
  team_color?: string
}

export interface LapTime {
  session_id: string
  rider_name: string
  rider_number: string
  lap_number: number
  lap_time: string
  lap_time_ms: number
  t1?: string
  t1_ms?: number
  t2?: string
  t2_ms?: number
  t3?: string
  t3_ms?: number
  t4?: string
  t4_ms?: number
  speed?: number
  is_best_lap: number
  year: number
  event_id: string
  category_id: string
  event_name: string
  circuit_name: string
  session_type: string
}

export interface RaceTracePoint {
  rider_name: string
  lap_number: number
  gap_s: number
}

export interface BestLap {
  rider_name: string
  lap_time: string
  lap_time_ms: number
  t1_ms?: number
  t2_ms?: number
  t3_ms?: number
  t4_ms?: number
  speed?: number
}

export interface LapTimesResponse {
  laps: LapTime[]
  raceTrace: RaceTracePoint[]
  bestLaps: BestLap[]
  riders: string[]
}

export interface TeamRider {
  year: number
  category_id: string
  category_name: string
  team_name: string
  team_color?: string
  rider_full_name: string
  rider_number: string
}
