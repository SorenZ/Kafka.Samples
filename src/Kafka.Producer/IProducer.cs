using System.Threading.Tasks;

namespace Kafka.Producer
{
    public interface IProducer
    {
        Task Send(object @event, string topic);
    }
}
