namespace MarketDataCollector.Profiler.Core.Interfaces;

/// <summary>Ввод-вывод консоли с цветовым оформлением и маппингом на уровни логирования.</summary>
public interface IConsoleUI
{
    /// <summary>Печатает заголовок секции в рамке из символов '='.</summary>
    void SectionHeader(string title);

    /// <summary>Информационное сообщение (Information).</summary>
    void Info(string message);

    /// <summary>Предупреждение (Warning).</summary>
    void Warn(string message);

    /// <summary>Успешное действие (Information).</summary>
    void Ok(string message);

    /// <summary>Ошибка (Error).</summary>
    void Error(string message);

    /// <summary>Детальное сообщение (Debug).</summary>
    void Detail(string message);
}
