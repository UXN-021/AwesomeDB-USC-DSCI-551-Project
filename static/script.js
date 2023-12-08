async function post(url, data){
    const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-type': 'application/json'
        },
        body: JSON.stringify(data)
    });
    const resData = await response.text()
    return resData;
}

function display(result) {
    const resultDisplay = document.getElementById("resultDisplay");
    console.log(result)
    resultDisplay.innerHTML = result;
}

function getEngineType() {
    const isRelational = document.getElementById("relationalCheck").checked;
    if (isRelational) {
        return "relational";
    } else {
        return "nosql";
    }
}

async function projection() {
    const engine = getEngineType();
    const projectionTableName = document.getElementById("projectionTableName").value;
    const projectionFields = document.getElementById("projectionFields").value;
    data = {
        table_name: projectionTableName,
        fields: projectionFields,
        engine: engine
    }
    try {
        res = await post("/projection", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}

async function filtering() {
    const engine = getEngineType();
    const filteringTableName = document.getElementById("filteringTableName").value;
    const filteringFields = document.getElementById("filteringFields").value;
    const filteringCondition = document.getElementById("filteringCondition").value;
    data = {
        table_name: filteringTableName,
        fields: filteringFields,
        condition: filteringCondition,
        engine: engine
    }
    try {
        res = await post("/filtering", data)
        display(res)
    } catch(err) {
        console.log(err)
    }
}