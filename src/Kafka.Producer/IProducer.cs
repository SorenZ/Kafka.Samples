using System.Threading.Tasks;

namespace Kafka.Producer
{
    public interface IProducer
    {
        Task Send(string message, string topic);
    }
}
