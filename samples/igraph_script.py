from igraph import *
from sys import *

arg = sys.argv[1]
arg = "/home/alex/random.gml"
print "loading " + arg + "..." 

g = Graph.Read_GML(arg)
print "loading complete" 

print "configuring visual style..."

#color_dict = {-1: "black", 0: "blue", 1: "red", 2: "green", 3: "pink", 4: "yellow", 5: "chocolate", 6: "dark orange", 7: "gray", 8: "maroon", 9: "purple", 10: "aqua", 11: "brown", 12: "dark cyan", 13: "dark green", 14: "dark magenta", 15: "fuchsia"}
color_dict = {-1: "#000000", 0: "#0000ff", 1: "#ff0000", 2: "#00ff00", 3: "#ffc0cb", 4: "#ffff00", 5: "#d2691e", 6: "#ff8c00", 7: "#a9a9a9", 8: "#b03060", 9: "#9370db", 10: "#00ffff", 11: "#a52a2a", 12: "#008b8b", 13: "#006400", 14: "#8b008b", 15: "#ff00ff"}
visual_style = {}
print "\tvertex sizes..."
visual_style["vertex_size"] = [8] * g.vcount()
print "\tvertex labels..."
visual_style["vertex_label"] = [""] * g.vcount()
print "\tvertex colors..."
#visual_style["vertex_color"] = ["#000000"] * g.vcount()
#visual_style["vertex_color"] = [color_dict[color] for color in g.vs["color"]]
print "\tedge colors..."
#visual_style["edge_color"] = ["#000000"] * g.vcount()
#visual_style["edge_color"] = [color_dict[color] for color in g.es["color"]] 
print "\tedge weights..."
visual_style["edge_width"] = 1
print "\tlayout..."

#large graph
visual_style["layout"] = g.layout("lgl")
 
#forced directed
#visual_style["layout"] = g.layout("fr")
 
#distributed recursive (large graph)
#visual_style["layout"] = g.layout("drl")
 
#forced directed
#visual_style["layout"] = g.layout("grid_fr") 

print "\twindow size..."
visual_style["bbox"] = (1500, 1500)
print "\twindow margin..."
visual_style["margin"] = 20

print "configuring visual style complete"

plot(g, **visual_style)
plot(g, "random.png", **visual_style)
plot(g, "_graph_m14b_2_150_lgl.svg", **visual_style)
