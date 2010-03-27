from igraph import *

resultsFolder = "/home/alex/Dropbox/Neo_Thesis_Private/"
algoFolders = ["Results DiDiC Mem",
               "Results DiDiC Mem Exp Paper",
               "Results DiDiC Mem Exp Sync"]
    
def viz_results_uk():
    settingsFolders = ["uk 2 Opt Balanced T11 B11",
                       "uk 2 Opt Random T11 B11",
                       "uk 16 Base Balanced T11 B11",
                       "uk 16 Base Random T11 B11",
                       "uk 16 Opt Balanced T11 B11",
                       "uk 16 Opt Random T11 B11"]
    settingsFiles = ["uk.2.500.gml",
                     "uk.2.500.gml",
                     "uk.16.500.gml",
                     "uk.16.500.gml",
                     "uk.16.500.gml",
                     "uk.16.500.gml"]
    
    for algoFolder in algoFolders:
        for index in range(len(settingsFolders)):
            gmlFile = resultsFolder + algoFolder + "/" + settingsFolders[index] + "/" + settingsFiles[index]
            imgFile = algoFolder + "-" + settingsFolders[index]
            viz_result(gmlFile, imgFile, imgLayout="lgl")

def viz_results_tree():
    settingsFolders = ["tree-4000-20 2 Opt Balanced T11 B11",
                       "tree-4000-20 2 Opt Random T11 B11",
                       "tree-4000-20 16 Base Balanced T11 B11",
                       "tree-4000-20 16 Base Random T11 B11",
                       "tree-4000-20 16 Opt Balanced T11 B11",
                       "tree-4000-20 16 Opt Random T11 B11"]
    settingsFiles = ["tree-4000-20.2.500.gml",
                     "tree-4000-20.2.500.gml",
                     "tree-4000-20.16.500.gml",
                     "tree-4000-20.16.500.gml",
                     "tree-4000-20.16.500.gml",
                     "tree-4000-20.16.500.gml"]
    
    for algoFolder in algoFolders:
        for index in range(len(settingsFolders)):
            gmlFile = resultsFolder + algoFolder + "/" + settingsFolders[index] + "/" + settingsFiles[index]
            imgFile = algoFolder + "-" + settingsFolders[index]
            viz_result(gmlFile, imgFile, imgLayout="fr")

def viz_result(gmlFile, imgFile, imgLayout="fr"):
    print "loading " + gmlFile + "..." 

    g = Graph.Read_GML(gmlFile)
    print "loading complete" 

    print "configuring visual style..."

    #color_dict = {-1: "black", 0: "blue", 1: "red", 2: "green", 3: "pink", 4: "yellow", 5: "chocolate", 6: "dark orange", 7: "gray", 8: "maroon", 9: "purple", 10: "aqua", 11: "brown", 12: "dark cyan", 13: "dark green", 14: "dark magenta", 15: "fuchsia"}
    color_dict = {-1: "#000000", 0: "#0000ff", 1: "#ff0000", 2: "#00ff00", 3: "#ffc0cb", 4: "#ffff00", 5: "#d2691e", 6: "#ff8c00", 7: "#a9a9a9", 8: "#b03060", 9: "#9370db", 10: "#00ffff", 11: "#a52a2a", 12: "#008b8b", 13: "#006400", 14: "#8b008b", 15: "#ff00ff"}
    visual_style = {}
    print "\tvertex sizes..."
    visual_style["vertex_size"] = [5] * g.vcount()
    print "\tvertex labels..."
    visual_style["vertex_label"] = [""] * g.vcount()
    print "\tvertex colors..."
    #visual_style["vertex_color"] = ["#000000"] * g.vcount()
    visual_style["vertex_color"] = [color_dict[color] for color in g.vs["color"]]
    print "\tedge colors..."
    #visual_style["edge_color"] = ["#ff0000"] * g.vcount()
    visual_style["edge_color"] = [color_dict[color] for color in g.es["color"]] 
    print "\tedge width..."
    visual_style["edge_width"] = 1
    print "\tlayout..."
    
    #large graph = "lgl"
    #forced directed = "fr"     
    #distributed recursive (large graph) = "drl"     
    #forced directed = "grid_fr"
    #circular = "circle"
    visual_style["layout"] = g.layout(imgLayout)    
    
    print "\twindow size..."
    visual_style["bbox"] = (1500, 1500)
    print "\twindow margin..."
    visual_style["margin"] = 20
    
    print "configuring visual style complete"
    
    print "saving png:" + imgFile + ".png..."
    plot(g, imgFile + ".png", **visual_style)
    
#    print "saving svg:" + imgFile + ".svg..."
#    plot(g, imgFile + ".svg", **visual_style)
    
    print "done"
    print "---"
