using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace VkOmskPromo
{
    public interface IUserIdGen
    {
        int TotalCount { get; }
        bool HasMore();
        int Peek();
        void Pop();
    }

    public class AllUserIdGen : IUserIdGen
    {
        private const int _start = 1;
        private int _end;
        private int _next = _start;
        public int TotalCount { get; private set; }

        public AllUserIdGen(int end)
        {
            _end = end;
            TotalCount = end;
        }

        public bool HasMore()
        {
            return _next <= _end;
        }

        public int Peek()
        {
            return _next;
        }

        public void Pop()
        {
            _next++;
        }
    }

    public class ListUserIdGen : IUserIdGen
    {
        private Queue<int> _ids;
        public int TotalCount { get; private set; }

        public ListUserIdGen(List<int> ids)
        {
            _ids = new Queue<int>();
            foreach(int id in ids)
            {
                _ids.Enqueue(id);
            }
            TotalCount = ids.Count;
        }

        public bool HasMore()
        {
            return _ids.Count > 0;
        }

        public int Peek()
        {
            return _ids.Peek();
        }

        public void Pop()
        {
            _ids.Dequeue();
        }
    }

    public class AgeBucket
    {
        private int _minAge;
        private int _maxAge;
        public StreamWriter IdsOutput;

        public AgeBucket(int minAge, int maxAge, StreamWriter idsOutput)
        {
            _minAge = minAge;
            _maxAge = maxAge;
            IdsOutput = idsOutput;
        }

        public bool Contains(DateTimeOffset birthDate)
        {
            var now = DateTimeOffset.Now;
            return birthDate.AddYears(_minAge) <= now && now < birthDate.AddYears(_maxAge + 1);
        }
    }

    public class User
    {
        public const string CsvHeader = "Id;FirstName;LastName;BYear;BMonth;BDay;City";
        public int Id;
        public string FirstName;
        public string LastName;
        public DateTimeOffset BirthDate;
        public int City;

        public string ToCsvString()
        {
            return $"{Id};{EscapeCsv(FirstName)};{EscapeCsv(LastName)};{BirthDate.Year};{BirthDate.Month};{BirthDate.Day};{City}";
        }

        private static string EscapeCsv(string s)
        {
            return s.Replace(",", "").Replace(";", "").Replace("\"", "");
        }
    }

    public class UsersFetcher
    {
        private static readonly long ActiveEpochStart =
            DateTimeOffset.Now.Subtract(Config.InactiveUserOffline).ToUnixTimeSeconds();
        private const string BaseUrl =
            "https://api.vk.com/method/users.get?fields=city,last_seen,bdate&user_ids=";

        private StreamWriter _log;
        private StreamWriter _idsOutput;
        private StreamWriter _failedIdsOutput;
        private IUserIdGen _userIdGen;
        private HttpClient _httpClient;
        private List<AgeBucket> _ageBuckets;
        private int _idsSucceeded;
        private int _idsFailed;
        private int _idsProcessed { get { return _idsSucceeded + _idsFailed; } }

        public UsersFetcher(
            IUserIdGen userIdGen,
            StreamWriter log,
            StreamWriter idsOutput,
            StreamWriter failedIdsOutput,
            HttpClient httpClient,
            List<AgeBucket> ageBuckets)
        {
            _log = log;
            _idsOutput = idsOutput;
            _failedIdsOutput = failedIdsOutput;
            _userIdGen = userIdGen;
            _httpClient = httpClient;
            _ageBuckets = ageBuckets;
        }

        public async Task Run()
        {
            var semaphore =
                new SemaphoreSlim(Config.TargetConcurrentRequests, Config.TargetConcurrentRequests);
            var startTime = DateTime.UtcNow;
            var tasks = new List<Task>();
            lock(_log)
            {
                _log.WriteLine(DateTime.Now.ToString());
                _log.WriteLine("Started");
            }
            while (_userIdGen.HasMore())
            {
                await semaphore.WaitAsync();
                tasks.Add(Task.Run(async () =>
                {
                    await ProcessOneRequest(startTime);
                    semaphore.Release();
                }));
            }
            await Task.WhenAll(tasks);
            lock(_log)
            {
                _log.WriteLine(DateTime.Now.ToString());
                _log.WriteLine("Finished");
            }
        }

        private async Task ProcessOneRequest(DateTime startTime)
        {
            var urlAndIds = GetUrlAndIds();
            if (urlAndIds == null)
            {
                return;
            }
            var url = urlAndIds.Item1;
            var ids = urlAndIds.Item2;
            try
            {
                var response = await _httpClient.GetAsync(url);
                var responseContent = await response.Content.ReadAsStringAsync();
                var filteredUsers = FilterResponse(responseContent);
                
                var usersAndIdsOutputByAge = _ageBuckets.Select(
                    bucket => new Tuple<IEnumerable<User>, StreamWriter>(
                        filteredUsers.Where(user => bucket.Contains(user.BirthDate)),
                        bucket.IdsOutput));
                foreach (var ageBucket in usersAndIdsOutputByAge)
                {
                    var users = ageBucket.Item1;
                    var idsOutput = ageBucket.Item2;
                    lock(idsOutput)
                    {
                        foreach(var user in users)
                        {
                            idsOutput.WriteLine(user.ToCsvString());
                        }
                        idsOutput.Flush();
                    }
                }

                lock(_log)
                {
                    Interlocked.Add(ref _idsSucceeded, ids.Count);
                    ReportStatus(startTime);
                }
            }
            catch(Exception ex)
            {
                lock(_log)
                {
                    Interlocked.Add(ref _idsFailed, ids.Count);
                    ReportStatus(startTime);
                    _log.WriteLine(ex.ToString());
                    _log.Flush();
                    lock(_failedIdsOutput)
                    {
                        foreach(var id in ids)
                        {
                            _failedIdsOutput.WriteLine(id.ToString());
                        }
                        _failedIdsOutput.Flush();
                    }
                }
            }
        }

        private void ReportStatus(DateTime startTime)
        {
            _log.WriteLine(DateTime.Now.ToString());
            _log.WriteLine(string.Format(
                "{0:0.00}% processed ({1} = {2} completed + {3} failed)",
                _idsProcessed * 100.0 / _userIdGen.TotalCount,
                _idsProcessed,
                _idsSucceeded,
                _idsFailed));
            var elapsedTime = DateTime.UtcNow - startTime;
            if (_idsSucceeded != 0)
            {
                var totalMs = elapsedTime.TotalMilliseconds * _userIdGen.TotalCount / _idsProcessed;
                var totalTime = TimeSpan.FromMilliseconds(totalMs);
                var finishTime = startTime + totalTime;
                _log.WriteLine("Estimated finish: {0}", finishTime.ToLocalTime());
                _log.Flush();
            }
        }

        private Tuple<string, List<int>> GetUrlAndIds()
        {
            lock(_userIdGen)
            {
                if (!_userIdGen.HasMore())
                {
                    return null;
                }
                var url = BaseUrl + _userIdGen.Peek().ToString();
                var ids = new List<int> { _userIdGen.Peek() };
                _userIdGen.Pop();

                while (_userIdGen.HasMore() &&
                    url.Length + 1 + Length(_userIdGen.Peek()) < Config.MaxUrlLength)
                {
                    url = url + "," + _userIdGen.Peek().ToString();
                    ids.Add(_userIdGen.Peek());
                    _userIdGen.Pop();
                }
                return new Tuple<string, List<int>>(url, ids);
            }
        }

        private static int Length(int i)
        {
            return (int)(Math.Log10(i)) + 1;
        }

        private List<User> FilterResponse(string response)
        {
            var jResponse = JObject.Parse(response)["response"] as JArray;
            var result = new List<User>();
            foreach(var jUser in jResponse)
            {
                if (jUser["city"] == null)
                {
                    continue;
                }
                else
                {
                    var city = jUser["city"].Value<int>();
                    if (city != Config.OmskCityId &&
                        !(Config.OmskRegionMinCityId <= city && city <= Config.OmskRegionMaxCityId))
                    {
                        continue;
                    }
                }
                if (jUser["last_seen"] == null ||
                    jUser["last_seen"]["time"].Value<int>() < ActiveEpochStart)
                {
                    continue;
                }
                DateTime birthDate;
                if (jUser["bdate"] == null)
                {
                    continue;
                }
                else
                {
                    var bDate = jUser["bdate"].Value<string>().Split('.');
                    if (bDate.Length != 3)
                    {
                        continue;
                    }
                    var day = int.Parse(bDate[0]);
                    var month = int.Parse(bDate[1]);
                    var year = int.Parse(bDate[2]);
                    try
                    {
                        birthDate = new DateTime(year, month, day);
                        if (!_ageBuckets.Any(bucket => bucket.Contains(birthDate)))
                        {
                            continue;
                        }
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        continue;
                    }
                }
                result.Add(new User 
                    {
                        Id = jUser["uid"].Value<int>(),
                        FirstName = jUser["first_name"].Value<string>(),
                        LastName = jUser["last_name"].Value<string>(),
                        BirthDate = birthDate,
                        City = jUser["city"].Value<int>()
                    });
            }
            return result;
        }
    }

    public class Program
    {
        public static void Main(string[] args)
        {
            var log = File.CreateText("log.txt");
            var idsOutput = File.CreateText("result.csv");
            idsOutput.Write(User.CsvHeader);
            idsOutput.Flush();
            var failedIdsPath = "failed_ids.txt";
            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Add("Accept-Language", "ru-RU,ru;q=1.0");

            var ageBuckets = new List<AgeBucket>();
            for (int age = Config.MinChildAge; age <= Config.MaxChildAge; age++)
            {
                ageBuckets.Add(CreateAgeBucket(age, age, $"{age}.csv"));
            }
            ageBuckets.Add(CreateAgeBucket(Config.MinAdultAge, Config.MaxAdultAge, "adults.csv"));

            var failedIdsOutput = File.CreateText(failedIdsPath);
            IUserIdGen userIdGen = new AllUserIdGen(Config.MaxUserId);
            var usersFetcher = new UsersFetcher(
                userIdGen, log, idsOutput, failedIdsOutput, httpClient, ageBuckets);
            log.WriteLine("Attempt 1");
            log.Flush();
            usersFetcher.Run().Wait();
            failedIdsOutput.Dispose();

            for (var attempt = 2;
                attempt <= Config.MaxAttempts && new FileInfo(failedIdsPath).Length > 0;
                attempt++)
            {
                var idsStream = File.OpenText(failedIdsPath);
                var idsStr = idsStream
                    .ReadToEnd()
                    .Split('\n')
                    .Where(s => s.Length > 0)
                    .Select(s => s.Trim())
                    .ToList();
                var idsInput = idsStr.Select(int.Parse).ToList();
                idsStream.Dispose();

                userIdGen = new ListUserIdGen(idsInput);
                failedIdsPath = $"failed_ids{attempt}.txt";
                failedIdsOutput = File.CreateText(failedIdsPath);
                usersFetcher = new UsersFetcher(
                    userIdGen, log, idsOutput, failedIdsOutput, httpClient, ageBuckets);
                log.WriteLine($"Attempt {attempt}");
                log.Flush();
                usersFetcher.Run().Wait();
                failedIdsOutput.Dispose();
            }

            log.Dispose();
            idsOutput.Dispose();
        }

        public static AgeBucket CreateAgeBucket(int minAge, int maxAge, string filename)
        {
            var ageBucket = new AgeBucket(minAge, minAge, File.CreateText(filename));
            ageBucket.IdsOutput.WriteLine(User.CsvHeader);
            ageBucket.IdsOutput.Flush();
            return ageBucket;
        }
    }
}
