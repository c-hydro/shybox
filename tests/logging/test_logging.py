# Main execution example
if __name__ == "__main__":

    from logging_handler import LoggingManager
    import logging

    # Configure once
    LoggingManager.setup(
        logger_folder="logs",
        logger_file="app.log",
        logger_format="%(levelname)-7s %(message)s",
        level=logging.INFO,
        arrow_base_len=3,
        arrow_prefix="-", arrow_suffix=">",
        warning_prefix="=", error_prefix="!",
        warning_dynamic=False,
        error_dynamic=True,
        warning_fixed_prefix="===> ",
        error_fixed_prefix=None,
    )

    log = LoggingManager(name="main", level=logging.INFO)

    # Replaces ArrowPrinter.arrow_main_break
    log.info_header(LoggingManager.rule_line("=", 78))

    # Headers / blanks
    log.info_header("RUN START", blank_before=True, underline=True)

    with log.span("Download batch", tag="dl"):
        log.info("Connect")
        log.warning("Field 'status' missing")  # ==> "===> Field ..." (fixed)
        log.info("Parsed 1200 records")
        try:
            1/0
        except ZeroDivisionError:
            log.exception("Crashed while computing metrics")  # depth-aware, error-style
        log.info("Batch done", end=True)

    log.info_header(LoggingManager.rule_line("=", 78), blank_after=True)