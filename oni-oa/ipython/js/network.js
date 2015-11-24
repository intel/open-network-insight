    // The code in this block is executed when the
    // d3.js library has been loaded.

    // First, we specify the size of the canvas containing
    // the visualization (size of the <div> element).
    var width = 1000,
        height = 650;

    // We create a color scale.
    var color = d3.scale.category10();
    var direct = d3.scale.category10();
    var swidth = d3.scale.log()
    var ldist = d3.scale.log()

    // We create a force-directed dynamic graph layout.
    var force = d3.layout.force()
        .charge(0) //.linkDistance(30)
        .size([width, height])
        .gravity(5);

    // Here's the part where we make the two graphs differ. Because
    // we're looking at the `linkDistance` property, that's what we
    // want to vary between the graphs. Most often this property is
    // set to a constant value for an entire visualization, but D3
    // also lets us define it as a function. When we do that, we
    // can set a different value for each link.

    force.linkDistance(function(link) {
       return ldist(link.bytes)*50;
    });


    // In the <div> element, we create a <svg> graphic
    // that will contain our interactive visualization.
    var svg = d3.select("#d3-example").select("svg")
    if (svg.empty()) {
        svg = d3.select("#d3-example").append("svg")
                    .attr("width", width)
                    .attr("height", height);
    }

    // We load the JSON file.
    d3.json("user/gbabb/nfgraph.json", function(error, graph) {
        // In this block, the file has been loaded
        // and the 'graph' object contains our graph.

        // We load the nodes and links in the force-directed
        // graph.
        force.nodes(graph.nodes)
            .links(graph.links)
            .start();

        // We create a <line> SVG element for each link
        // in the graph.
        var link = svg.selectAll(".link")
            .data(graph.links)
            .enter().append("line")
            .attr("class", "link")
			.style("stroke", function(d) {
			 	//The edge color depends on the flow dir.
				return d.source.level > 1 ? "darkgreen" : "lightgreen";
				//return direct(d.source.level);
			})
			.style("stroke-width", function(d) {
			 	//The edge width depends on the bytes.
				return (swidth(Math.abs(d.bytes)))/2;
			})
			;
        // We create a <circle> SVG element for each node
        // in the graph, and we specify a few attributes.
        var node = svg.selectAll(".node")
            .data(graph.nodes)
            .enter().append("circle")
            .attr("class", "node")
            .attr("r", 10)  // radius
            .style("fill", function(d) {
                // The node color depends on the level.
                return d.level > 0 ? "lightblue" : "orange";
            })
            .call(force.drag);

        // The name of each node is the node number.
        node.append("title")
            .text(function(d) { return d.name; });

        link.append("title")
            .text(function(d) { return d.bytes; });

        // We bind the positions of the SVG elements
        // to the positions of the dynamic force-directed graph,
        // at each time step.
        force.on("tick", function() {
            link.attr("x1", function(d) { return d.source.x; })
                .attr("y1", function(d) { return d.source.y; })
                .attr("x2", function(d) { return d.target.x; })
                .attr("y2", function(d) { return d.target.y; });

            node.attr("cx", function(d) { return d.x; })
                .attr("cy", function(d) { return d.y; });
        });
    });
