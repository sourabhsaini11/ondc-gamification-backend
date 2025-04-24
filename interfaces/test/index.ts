export type Gender = "male" | "female" | "other"

export interface IUser {
  name: string
  age: number
  gender: Gender
}
export interface NormalizedRow {
  order_id: string
  order_status: string
  timestamp_created: string
  total_price: number
  phone_number: string
  timestamp_updated?: string
}

export interface OrderRecord {
  uid: string
  order_id: string
  order_status: string
  timestamp_created: Date
  timestamp_updated: Date
  buyer_app_id: string
  buyer_name: string
  total_price: number
}
export interface FullProcessedOrderRecord
  extends Omit<OrderRecord, "timestamp_created" | "timestamp_updated" | "uid" | "order_status"> {
  uid: string
  order_id: string
  order_status: string
  timestamp_created: string
  timestamp_updated: string
  game_id: string
  // points: number
  entry_updated: boolean
  streak_maintain: boolean
  highest_gmv_for_day: boolean
  highest_orders_for_day: boolean
  same_day_order_count?: number
  streak_count?: number
  // gmv: number
  last_streak_date: any
}

export interface OrderStatusValidationResult {
  success: boolean
  message?: string
}

export interface cancelledOrders {
  order_id: string
}

export interface aggregatedData {
  game_id: string
  total_points: string
  total_orders: string
  total_gmv: string
}
export interface NormalizedRow {
  [key: string]: string | undefined
}
