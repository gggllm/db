let memo = [];

function calculateJoinSize(metaData, {tableName, tableName2, column, column2}) {

    let meta1 = metaData[tableName] || memo[tableName];
    let meta2 = metaData[tableName2] || memo[tableName2];
    let unique1 = meta1[column].unique;
    let size1 = meta1.size;
    let size2 = meta2.size;
    let unique2 = meta2[column2].unique
    let size = size1 * size2 / Math.min(unique1, unique2) / (unique1 * unique2)
    memo[tableName+column+tableName2+column2]={
        size:size,

    }
    return size
}
