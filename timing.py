from sootty import WireTrace, Visualizer, Style
from sootty.utils import evcd2vcd
import time

t0 = time.time()
# Create wiretrace object from vcd file:
wiretrace = WireTrace.from_vcd("example/example1.vcd")
t1 = time.time()

total = t1 - t0
print(f"Time taken: {total:.4f}s")

t2 = time.time()
# Convert wiretrace to svg:
image = Visualizer(Style.Dark).to_svg(wiretrace, start=0, length=8, wires='D0 D1' )
t3 = time.time()

total2 = t3 - t2
print(f"Time taken2: {total2:.4f}s")

# Display to stdout:
image.display()

# Manually convert EVCD file to VCD file:
# with open('myevcd.evcd', 'rb') as evcd_stream:
#     vcd_reader = evcd2vcd(evcd_stream)
#     with open('myvcd.vcd', 'wb') as vcd_stream:
#         vcd_stream.write(vcd_reader.read())