module.exports = function (RED) {
    var createQueue = require('edge-js').func(`
    #r "System.Messaging.dll"
    
    using System;
    using System.Messaging;
    using System.IO;
    using System.Threading.Tasks;
    
    public class Startup
    {
        public async Task<object> Invoke(dynamic input)
        {
            var log = (Func<object,Task<object>>) input.log;
            var messageReceived = (Func<object,Task<object>>) input.messageReceived;
            var shouldStop = false;
            var name = (string) input.name;
            var waitSeconds = (int) input.waitSeconds;

            log("new queue: " + name);
        
            var readTask = new Task(async () => {
                var queue = new MessageQueue(name);
    
                if (!MessageQueue.Exists(name)) MessageQueue.Create(name);
    
                var enumerator = queue.GetMessageEnumerator2();
    
                while (!shouldStop) {
                    if (enumerator.MoveNext(TimeSpan.FromSeconds(waitSeconds)))
                    {
                        var message = enumerator.Current;
                        
                        using (var reader = new StreamReader(message.BodyStream))
                        {
                            var body = reader.ReadToEnd();
        
                            try {
                                var shouldRemove = (bool) await messageReceived(body);

                                if (shouldRemove) enumerator.RemoveCurrent();
                            } catch (Exception ex) {
                                log(ex.Message);
                            }
                        }
                    }
                }
            });
    
            return new {
                stop = new Func<object,Task<object>> ((stopParam) => {
                    log("Stop listening queue " + name);
                    
                    shouldStop = true;

                    return Task.FromResult((object) null);
                }),
                start = new Func<object,Task<object>> ((startParam) => {
                    log("Start listening queue " + name);

                    readTask.Start();

                    return Task.FromResult((object) null);
                })
            };
        }
    }
    `);

    function MsmqReceiveNode(config) {
        RED.nodes.createNode(this, config);
        var node = this;
        var queue;
        
        createQueue({
            name: config.queue,
            waitSeconds: 1,
            log: function (what, callback) {
                console.log(what);
                
                callback();
            },
            messageReceived: function (data, callback) {
                node.send({
                    payload: data
                });

                callback(null, true);
            }
        }, (err, proxy) => {
            if (err) return this.error(err);

            queue = proxy;

            try {
            queue.start(null, (err) => {
                if (err) this.error(err);   
            });
        } catch (err) {
            debugger;
        }
        });

        this.on('close', function () {
            // tidy up any state
            queue.stop(null, (err) => {
                if (err) this.error(err);
            });
        });

    }
    RED.nodes.registerType("msmq-receive", MsmqReceiveNode);
}
