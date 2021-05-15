package com.oskarro.enums

object VehicleType extends Enumeration {

  type VehicleType = Value

  // Assigning values
  val bus: VehicleType.Value = Value(1, "bus")
  val tram: VehicleType.Value = Value(2, "tram")

}
