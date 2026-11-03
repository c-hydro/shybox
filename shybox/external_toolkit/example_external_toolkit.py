# ----------------------------------------------------------------------------------------------------------------------
# libraries
from shybox.external_toolkit import check_external_toolkit
# ----------------------------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------------------------
# main
def main_external_toolkit():

    # check rfarm and prism libraries - start
    print('check rfarm and prism libraries ... ')

    # Verify dependency and install if missing
    check_external_toolkit("rfarm",auto_install=True, raise_error=True)

    # Check and install PRISM
    check_external_toolkit("prism",auto_install=True,raise_error=True)

    import rfarm
    import prism

    # check rfarm and prism libraries - end
    print('check rfarm and prism libraries ... INSTALLED')

# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# call script from external library
if __name__ == "__main__":
    main_external_toolkit()
# ----------------------------------------------------------------------------------------------------------------------
