#
# Copyright (c) 2025 ClearTax. All rights reserved.
#

import sys

from destination_prism import DestinationPrism


def run():
    DestinationPrism().run(sys.argv[1:])


if __name__ == "__main__":
    run()
