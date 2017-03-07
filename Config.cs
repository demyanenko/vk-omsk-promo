using System;

namespace VkOmskPromo
{
    public class Config
    {
        public const int MaxUserId = 418 * 1000 * 1000;
        public const int MinChildAge = 13;
        public const int MaxChildAge = 18;
        public const int MinAdultAge = 28;
        public const int MaxAdultAge = 45;
        public const int OmskCityId = 104;
        public const int OmskRegionMinCityId = 1145150;
        public const int OmskRegionMaxCityId = 1146700;
        public static readonly TimeSpan InactiveUserOffline = TimeSpan.FromDays(31);
        public const int TargetConcurrentRequests = 30;
        public const int MaxUrlLength = 4096;
        public const int MaxAttempts = 10;
    }
}