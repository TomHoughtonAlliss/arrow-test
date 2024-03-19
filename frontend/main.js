import { writeFileSync, readFileSync, read } from 'fs';
import { tableFromJSON, tableFromIPC, tableToIPC } from 'apache-arrow';

// Sample data as JSON
const data = [
    { foo: 1, bar: 1, baz: 'aa' },
    { foo: null, bar: null, baz: null },
    { foo: 3, bar: null, baz: null },
    { foo: 4, bar: 4, baz: 'bbb' },
    { foo: 5, bar: 5, baz: 'cccc' }
  ];

// Convert JSON to table
const table = tableFromJSON(data);

function getArrayFromJSON() 
{
  console.table(table.toArray()); 
}

// Convert table to IPC and write to file
const ipcTable = tableToIPC(table);
writeFileSync("simple.arrow", ipcTable);

function getArrayFromIPC() 
{
    const arrow = readFileSync('./backend/output.ipc');
    const table = tableFromIPC(arrow);
    console.table(table.toArray());
}


//getArrayFromJSON();
getArrayFromIPC();

const byteArray = readFileSync('bytearray.arrow');
const byteString = String.fromCharCode.apply(null, byteArray);
const jsonData = JSON.parse(byteString);
//const tab = tableFromIPC(byteArray);
//console.table(table.toString());
//console.log(jsonData);
