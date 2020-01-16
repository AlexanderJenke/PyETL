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
        console.log(rows[redRows[i] + j])
        console.log(rows[redRows[i] + j + 1])
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
    for (i = 0; i < tr.length; i++) {
            td = tr[i].getElementsByTagName("td")[0];
            if (td) {
                txtValue = td.textContent || td.innerText;
                if (txtValue.toUpperCase().indexOf(filter) > -1) {
                    tr[i].style.display = "";
                } else {
                    tr[i].style.display = "none";
                }
            }
    }
}

function openDialog() {
    var dialog = document.getElementById("dialog");

}