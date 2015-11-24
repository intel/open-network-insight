var diameter = 300,
    format = d3.format(",d"),
    color = d3.scale.category20();

var bubble = d3.layout.pack()
    .sort(null)
    .size([diameter, diameter])
    .padding(1.5);

var svg = d3.select("body").append("svg")
    .attr("width", diameter)
    .attr("height", diameter)
    .attr("class", "bubble");

d3.json("json/ldagraph.json", function(error, root) {
  var node = svg.selectAll(".node")
     .data(bubble.nodes(classes(root)))
     //.filter(function(d) { return d.group == 1; }))
    .enter().append("g")
      .attr("class", "node")
      .attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });

  node.append("title")
      .text(function(d) { return d.group + ": " + format(d.size); });

  node.append("circle")
      .attr("r", function(d) { return d.r; })
       .on("click", csvClick)
      .style("fill", function(d) { return isNaN(d.group) ? "white": color(d.group) });

  node.append("text")
      .attr("dy", ".3em")
      .style("text-anchor", "middle")
      .text(function(d) { return d.name; })
      ;
});

// Returns a flattened hierarchy containing all leaf nodes under the root.
function classes(root) {
  var classes = [];

  function recurse(name, node) {
    if (node.children) node.children.forEach(function(child) { recurse(node.name, child); });
    else classes.push({group: node.group, name: node.name, size: node.size,value: node.value});
  }

  recurse(null, root);
  return {children: classes};
}

	function csvClick(d) {

				var fpath = "port" + d.name + ".csv"
				//alert(fpath)
				parent.document.getElementById('vizView').contentWindow.chord(fpath);
				return;

  	}
//d3.select(self.frameElement).style("height", (diameter+200) + "px");
