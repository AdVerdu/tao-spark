package org.corerda.entities

import scopt.OptionParser

// TODO - review to make an ADT for input params
case class InputParam(jobName: String = "example1", environment: String = "local")

object InputParam {
  val input = new OptionParser[InputParam]("tao-spark") {
    head("tao-spark", "0.1.0")

    opt[String]("program_name")
      .optional()
      .validate(value =>
        // FIXME - avoid this kind of validation
        if (value == "plastics" || value == "other") success
        else failure("Job not hardcoded")
      )
      .action((value, config) => config.copy(jobName = value))
      .text("Name of Job")

    opt[String]("environment")
      .optional()
      .validate(value =>
        if (value == "development" || value == "production" || value == "local") success
        else failure("Invalid environment. Allowed values: development, production, local")
      )
      .action((value, config) => config.copy(environment = value))
      .text("Environment the program will run in (development, production, local)")
  }
}
