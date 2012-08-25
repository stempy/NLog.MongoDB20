﻿using System;
using System.Collections.Generic;
using System.Configuration;
using MongoDB.Bson;
using MongoDB.Driver;
using NLog.Config;
using NLog.Targets;

namespace NLog.MongoDB
{
	[Target("MongoDB")]
	public sealed class MongoDBTarget : Target
	{
		public Func<IRepositoryProvider> GetProvider = () => new MongoServerProvider();

		public MongoDBTarget()
		{
			Fields = new List<MongoDBTargetField>();
		}

        #region Exposed Properties

		[ArrayParameter(typeof(MongoDBTargetField), "field")]
		public IList<MongoDBTargetField> Fields { get; private set; }

        public string ConnectionString { get; set; }

        public string ConnectionName { get; set; }

        public string Username { get; set; }

        public string Password { get; set; }

	    public string CollectionName { get; set; }

	    public bool UseCappedCollection { get; set; }

	    public bool CreateIdField { get; set; }

        public bool AppendFields { get; set; }

        public long? CappedCollectionSize { get; set; }

        public long? CappedCollectionMaxItems { get; set; }

        #region Defaulted Properties

        public string Host
		{
			get { return _Host ?? "localhost"; }
			set { _Host = value; }
		}
		private string _Host;

		public int Port	
		{
			get { return _Port ?? 27017; }
			set { _Port = value; }
		}
		private int? _Port;

		public string Database
		{
			get { return _Database ?? "NLog"; }
			set { _Database = value; }
		}
		private string _Database;

        #endregion

        #endregion

        #region Private Helpers

        internal IRepository GetRepository()
        {
            // We have a connection string name, grab this from the config.
            if (!string.IsNullOrWhiteSpace(this.ConnectionName))
            {
                if (ConfigurationManager.ConnectionStrings[this.ConnectionName] == null ||
                    string.IsNullOrWhiteSpace(ConfigurationManager.ConnectionStrings[this.ConnectionName].ConnectionString))
                    throw new MongoConnectionException("The connection string name specified was not found.");

                this.ConnectionString = ConfigurationManager.ConnectionStrings[this.ConnectionName].ConnectionString;
            }

            MongoUrlBuilder mongoUrlBuilder;
            // We have a connection string
            if (!string.IsNullOrWhiteSpace(this.ConnectionString))
            {
                mongoUrlBuilder = new MongoUrlBuilder(this.ConnectionString);

				if (string.IsNullOrEmpty(mongoUrlBuilder.DatabaseName))
                {
                    mongoUrlBuilder.DatabaseName = Database;
                }
            }
            // No connection strings at all, use the old method using the properties                    
            else
            {
                mongoUrlBuilder = new MongoUrlBuilder
	            {
		            DatabaseName = Database,
					Server = new MongoServerAddress(Host, Port)
	            };

	            if (HasCredentials)
					mongoUrlBuilder.DefaultCredentials = new MongoCredentials(Username, Password);
            }

            return GetProvider().GetRepository(mongoUrlBuilder.ToServerSettings(), mongoUrlBuilder.DatabaseName);
        }

	    private bool HasCredentials
	    {
	        get
	        {
	            return !string.IsNullOrWhiteSpace(this.Username) && !string.IsNullOrWhiteSpace(this.Password);
	        }
	    }

        internal BsonDocument BuildBsonDocument(LogEventInfo logEvent)
        {
            BsonDocument doc;

			if (Fields.Count == 0 || this.AppendFields)
			{
			    doc = BuildFullBsonDocument(logEvent);
			}
			else
			{
			    doc = new BsonDocument();
                if (this.CreateIdField) doc["_id"] = ObjectId.GenerateNewId();
			}

			foreach (var field in Fields)
			{
				if (field.Layout != null)
				{
					doc[field.Name] = field.Layout.Render(logEvent);
					continue;
				}

				var searchResult = logEvent.GetValue(field.Name);
				if (!searchResult.Succeded)
					throw new InvalidOperationException(string.Format("Invalid field name '{0}'.", field.Name));

				doc.AddField(field.Name, searchResult.Value);
			}

			return doc;
		}

        internal BsonDocument BuildFullBsonDocument(LogEventInfo logEvent)
		{
			var doc = new BsonDocument();
            
			if (CreateIdField)
				doc.AddField("_id", ObjectId.GenerateNewId());

			doc.AddField("sequenceID", logEvent.SequenceID);
			doc.AddField("timeStamp", logEvent.TimeStamp);
			doc.AddField("machineName", Environment.MachineName);
			doc.AddField("loggerName", logEvent.LoggerName);
			doc.AddField("message", logEvent.Message);
			doc.AddField("formattedMessage", logEvent.FormattedMessage);
			doc.AddField("level", logEvent.Level);
			doc.AddField("stackTrace", logEvent.StackTrace);
			doc.AddField("userStackFrame", logEvent.UserStackFrame);
			doc.AddField("UserStackFrameNumber", logEvent.UserStackFrameNumber);
			doc.AddField("exception", logEvent.Exception);
			doc.AddField("properties", logEvent.Properties);
			doc.AddField("parameters", logEvent.Parameters);

			return doc;
		}

        #endregion

#if DEBUG

        internal void TestWrite(LogEventInfo logEvent)
		{
			Write(logEvent);
		}

#endif

		protected override void Write(LogEventInfo logEvent)
		{
			using (var repository = GetRepository())
			{
			    string collectionName = logEvent.LoggerName;
                if (!string.IsNullOrEmpty(this.CollectionName)) collectionName = this.CollectionName;

			    repository.CheckCollection(collectionName, this.UseCappedCollection, this.CappedCollectionSize, this.CappedCollectionMaxItems, this.CreateIdField);
                repository.Insert(collectionName, BuildBsonDocument(logEvent));
			}
        }

    }
}
