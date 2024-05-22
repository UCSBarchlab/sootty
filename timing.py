from sootty import WireTrace, Visualizer, Style
from sootty.utils import evcd2vcd
import time

t0 = time.time()
# Create wiretrace object from vcd file:
wiretrace = WireTrace.from_vcd("example/example3.vcd")
t1 = time.time()

total = t1 - t0
print(f"Time taken: {total:.4f}s")

t2 = time.time()
# Convert wiretrace to svg:
for i in range(0, 100):
    image = Visualizer(Style.Dark).to_svg(wiretrace, start=4, length=26, wires='clk,rst_n,clk,rst_n,pc,pc_next,inst,immed,wreg,wdata,sdata,tdata,op,alu_in1,alu_in2,alu_out,zero,rdata,alu_src,alu_shift,branch,mem_to_reg,mem_read,mem_write,reg_dst,reg_write,addr,rdata,clk,rst_n,reg_write,wreg,wdata,sdata,tdata')
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