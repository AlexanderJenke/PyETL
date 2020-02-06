var asc = false;

function sortBack() {
    var table = document.getElementById("table");
    var rows = table.rows;
    var switching = true;
    var i = 1;
    while(switching) {
        switching = false;
        for (i = 1; i < rows.length - 1; i++) {
            var idx = rows[i].id;
            var idx_value = parseFloat(idx);
            var idy = rows[i + 1].id;
            var idy_value = parseFloat(idy);
            console.log(idx + " " + idy)
            shouldSwitch = false;
            if (idy_value < idx_value) {
                shouldSwitch = true;
                break;
            }
        }
        if (shouldSwitch) {
            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
            switching = true;
        }
    }

}

function sortBack2() {
	
	var table = document.getElementById("table");
	var rows = table.rows;
	var list = [];
	for (var i = 1; i < rows.length; i++) {
		list.push(rows[i].id);
	}
	list = list.sort(function(a,b) {return parseFloat(a) - parseFloat(b)});
	list2 = []
	for (var i = 0; i < list.length; i++) {
		var x = document.getElementById(list[i]);
		list2.push(x)
	}
	for (var i = 0; i < rows.length - 1; i++) {
		var x = list2[list2.length - i - 1];
		if (i % 2 != 0) {
			x.parentNode.insertBefore(x, rows[1]);
		} else {	
			x.parentNode.insertBefore(x, rows[1]);
		}
	}
	

}


function sort_timestamp() {
	
	var table = document.getElementById("table");
	var rows = table.rows;
	var list = [];
	for (var i = 1; i < rows.length; i+= 2) {
		//console.log(rows[i].cells);
		//var parts = rows[i].cells[3].innerText.split("/");
		//date = new Date(parts[2], parts[0], parts[1])
		list.push(rows[i]);
	}
	list = list.sort(function(a,b) {
        	var parts = a.cells[3].innerText.split("/");
		var date = new Date(parts[2], parts[0], parts[1])
		parts = b.cells[3].innerText.split("/");
		var date2 = new Date(parts[2], parts[0], parts[1])
		return date > date2 ? -1 : date2 > date ? 1 : 0;
		});
	console.log(list)
  

	for (var i = 0; i < list.length; i++) {
		var x = list[list.length - i - 1];
		var id = x.id;
		var id2 = id + ".5";
		y = document.getElementById(id2);
		x.parentNode.insertBefore(y, rows[1]);
		x.parentNode.insertBefore(x, rows[1]);
	}
}


function sort_fab() {
	
	var table = document.getElementById("table");
	var rows = table.rows;
	var list = [];
	for (var i = 1; i < rows.length; i+= 2) {
		list.push(rows[i]);
	}
	list = list.sort(function(a,b) {
		var a_str = a.cells[4].innerText.toLowerCase();
		var b_str = b.cells[4].innerText.toLowerCase();
		return a_str > b_str ? -1 : b_str > a_str ? 1 : 0;
		});
	console.log(list)
  

	for (var i = 0; i < list.length; i++) {
		var x = list[list.length - i - 1];
		var id = x.id;
		var id2 = id + ".5";
		y = document.getElementById(id2);
		x.parentNode.insertBefore(y, rows[1]);
		x.parentNode.insertBefore(x, rows[1]);
	}
}

function sortTable() {
    var table = document.getElementById("table");
    var switching = true;
    var redRows = [];
    var rows = table.rows;
    for (var i = 0; i < rows.length; i++) {
        row = rows[i];
        if (row.className === 'table-danger') {
            redRows.push(i);
        }
    }
    var j = 0;
    for (var i = redRows.length - 1; i >= 0; i--) {
        rows[1].parentNode.insertBefore(rows[redRows[i] + j], rows[1]);
        console.log(rows[redRows[i] + j])
        rows[1].parentNode.insertBefore(rows[redRows[i] + j + 1], rows[2]);
        j += 2;
    }
}

function togglePatient(patientID) {
        $(".collapse").collapse('hide');
        $("#" + patientID).collapse('show');
}

function filterTableById() {
    var input, filter, table, tr, td, i, txtValue;
    input = document.getElementById("idFilter");
    filter = input.value.toUpperCase();
    table = document.getElementById("table");
    tr = table.getElementsByTagName("tr");
    for (i = 1; i < tr.length - 1; i += 2) {
            td = tr[i].getElementsByTagName("td")[0];
            if (td) {
                txtValue = td.textContent || td.innerText;
                if (txtValue.toUpperCase().indexOf(filter) > -1) {
                    tr[i].style.display = "";
                    tr[i + 1].style.display = "";
                } else {
                    tr[i].style.display = "none";
                    tr[i + 1].style.display = "none";
                }
            }
    }
}

function openDialog() {
    var dialog = document.getElementById("dialog");

}

