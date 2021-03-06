package io.github.interestinglab.waterdrop.config

object CommandLineUtils {

  /**
   * command line arguments parser.
   * */
  val parser = new scopt.OptionParser[CommandLineArgs]("start-waterdrop.sh") {
    head("Waterdrop", "1.0.0")

    opt[String]('c', "config").required().action((x, c) => c.copy(configFile = x)).text("config file")
    opt[Unit]('t', "check").action((_, c) => c.copy(testConfig = true)).text("check config")
    opt[String]('e', "deploy-mode")
      .optional()
      .withFallback(() => "client")
      .action((x, c) => c.copy(deployMode = x))
      .validate(x => if (Common.isModeAllowed(x)) success else failure("deploy-mode: " + x + " is not allowed."))
      .text("spark deploy mode")
    opt[String]('m', "master")
      .optional()
      .withFallback(() => "local[*]")
      .text("spark master")
    opt[String]('i', "variable")
      .optional()
      .text("variable substitution, such as -i city=beijing, or -i date=20190318")
      .maxOccurs(Integer.MAX_VALUE)
      .action((x, c) => c.copy(variables = c.variables ++ List(x)))
    opt[String]('q', "queue")
      .optional()
      .text("spark queue")
    opt[Int]('s', "sleep")
      .optional()
      .action((x, c) => c.copy(sleepSeconds = Option(x)))
      .text("whether sleep specified seconds before stop spark (to get a chance to investigate spark web ui)")
  }
}
