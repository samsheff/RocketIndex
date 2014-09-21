var NNTP = require('nntp'),
    inspect = require('util').inspect,
    elasticsearch = require('elasticsearch');

var client = new elasticsearch.Client({
    host: 'localhost:9200'
});

//NNTP Protocol Handlers
var c = new NNTP();
c.on('ready', function() {
  c.group('alt.binaries.town', function(err, count, low, high) {
    if (err) throw err;

    c.article(function(err, n, id, headers, body) {
      indexArticle(id);
    });
  });
});

c.on('error', function(err) {
  console.log('Error: ' + err);
});

c.on('close', function(had_err) {
  console.log('Connection closed');
});

c.connect({
  host: '',
  user: '',
  password: ''
});

// Recursive Function for iterating through groups
function indexArticle (messageId) {
  c.article(function(err, n, id, headers, body) {
    if (err) throw err;
    console.log('Indexing Article #' + n);

    client.index({
      index: 'usenet',
      type: 'nzb',
      id: id,
      body: createElasticsearchObject(n, id, headers, body)
    }, function (error, response) {
         c.next(function (err, articleNum, msgId) {
            if (err) throw err;
            indexArticle(msgId);
         });
    });
  });  
}

// Helper Function for creating ES body
function createElasticsearchObject (messageNum, messageId, messageHeaders, messageBody) 
{
  // Message Headers are already an object, so we just tack on the other fields and return
  esObject = messageHeaders;

  esObject['messageId'] = messageId;
  esObject['messageNum'] = messageNum;
  //esObject['messageBody'] = messageBody;

  return esObject;
}
