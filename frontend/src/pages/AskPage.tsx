import { useState, useRef, useEffect } from 'react';
import Markdown from 'react-markdown';
import { postQuery, getSeriesLabel } from '@/lib/api';
import type { QueryResponse } from '@/lib/api';
import { useLanguage } from '@/lib/LanguageContext';
import { useDomain, localize } from '@/lib/domain';
import type { Translations } from '@/lib/i18n';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  response?: QueryResponse;
  isLoading?: boolean;
  error?: string;
}

function TierBadge({ tier, t }: { tier: 'direct_lookup' | 'full_llm'; t: Translations }) {
  const isT1 = tier === 'direct_lookup';
  return (
    <span
      className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider border ${
        isT1
          ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30'
          : 'bg-brand-500/20 text-brand-400 border-brand-500/30'
      }`}
    >
      {isT1 ? t.ask.tier1 : t.ask.tier3}
    </span>
  );
}

function DataCitations({ response, t }: { response: QueryResponse; t: Translations }) {
  if (response.data_points.length === 0) return null;

  return (
    <div className="mt-3 rounded-lg border border-slate-700/50 bg-slate-900/50 p-3">
      <p className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider mb-2">
        {t.ask.dataPoints}
      </p>
      <div className="space-y-1">
        {response.data_points.map((dp, i) => (
          <div key={i} className="flex items-center justify-between text-xs">
            <span className="text-slate-400">{getSeriesLabel(dp.series)}</span>
            <span className="text-slate-300 font-medium">
              {dp.value.toLocaleString('en-US', { maximumFractionDigits: 4 })}
            </span>
            <span className="text-slate-500">{dp.date}</span>
          </div>
        ))}
      </div>
      {response.sources.length > 0 && (
        <p className="text-[10px] text-slate-600 mt-2">
          {t.ask.sources}: {response.sources.join(', ')}
        </p>
      )}
    </div>
  );
}

export function AskPage() {
  const { language, t } = useLanguage();
  const cfg = useDomain();
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [questionCount, setQuestionCount] = useState(0);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scopeDesc = localize(cfg.ai.scope_description, language);
  const indicators = localize(cfg.ai.example_indicators, language);
  const sourceNames = cfg.data_sources.map((ds) => ds.name).join(', ');

  const subtitle = language === 'pt'
    ? `Faça perguntas sobre ${scopeDesc} em linguagem natural.`
    : `Ask questions about ${scopeDesc} in natural language.`;

  const placeholder = language === 'pt'
    ? `Pergunte sobre ${indicators}...`
    : `Ask about ${indicators}...`;

  const disclaimer = language === 'pt'
    ? `Respostas geradas por IA podem ser imprecisas. Verifique os dados com fontes oficiais (${sourceNames}).`
    : `AI-generated responses may be inaccurate. Verify data with official sources (${sourceNames}).`;

  const suggestions = language === 'pt'
    ? [
        'Compare o atual ciclo de alta da SELIC com o período de 2015-2016.',
        'Existe alguma divergência entre as projeções do Focus para o IPCA e os juros reais (IPCA+) hoje?',
        'Como a variação do dólar (USD/BRL) nos últimos 6 meses impactou a inflação (IPCA)?',
        'Considerando a carga de energia (ONS), a atividade econômica (IBC-Br) está acelerando?',
        'Quais indicadores macroeconômicos estão sinalizando anomalias estatísticas nesta semana?',
        'Qual a relação histórica entre a inadimplência PJ e a taxa de desemprego?',
        'Como o spread entre o Tesouro Prefixado Longo e Curto se comportou no último trimestre?',
        'Faça um resumo executivo dos riscos fiscais analisando a Dívida/PIB e o Resultado Primário.',
        'Existem sinais de recessão analisando o IBC-Br, produção industrial e vendas no varejo?',
        'Com base na inflação e câmbio atuais, o que o mercado precifica (Focus) para a SELIC?',
      ]
    : [
        'Compare the current SELIC cycle with the 2015-2016 environment.',
        'Is there a divergence between Focus IPCA expectations and market real yields (IPCA+)?',
        'How has the USD/BRL variation over the last 6 months impacted inflation (IPCA)?',
        'Considering ONS electricity load, is economic activity (IBC-Br) accelerating?',
        'Which macroeconomic indicators are flashing statistical anomalies this week?',
        'What is the historical relationship between corporate default rates and unemployment?',
        'How has the spread between long and short Treasury yields behaved in the last quarter?',
        'Provide an executive summary of fiscal risks analyzing Debt/GDP and Primary Balance.',
        'Are there recession signals if we jointly analyze IBC-Br, industrial production, and retail?',
        'Based on current inflation and FX, what is the market pricing (Focus) for the SELIC rate?',
      ];

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const question = input.trim();
    if (!question || isSubmitting) return;

    setInput('');
    setQuestionCount((c) => c + 1);

    const userMsg: Message = { role: 'user', content: question };
    const loadingMsg: Message = { role: 'assistant', content: '', isLoading: true };

    setMessages((prev) => {
      const updated = [...prev, userMsg, loadingMsg];
      if (updated.length > 20) {
        return updated.slice(updated.length - 20);
      }
      return updated;
    });

    setIsSubmitting(true);

    try {
      const response = await postQuery(question, language);
      setMessages((prev) =>
        prev.map((m) =>
          m.isLoading
            ? { role: 'assistant', content: response.answer, response }
            : m,
        ),
      );
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Unknown error';
      setMessages((prev) =>
        prev.map((m) =>
          m.isLoading
            ? { role: 'assistant', content: '', error: errorMsg }
            : m,
        ),
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  const dailyUsedText = t.ask.dailyUsed
    .replace('{count}', String(questionCount))
    .replace('{limit}', '10');

  return (
    <div className="min-h-[calc(100vh-3.5rem)] flex flex-col max-w-3xl mx-auto px-4 sm:px-6">
      {/* Header */}
      <header className="py-6">
        <h1 className="text-2xl font-bold text-slate-100">{t.ask.title}</h1>
        <p className="text-sm text-slate-500 mt-1">
          {subtitle}
        </p>
      </header>

      {/* Rate limit indicator */}
      <div className="mb-4 text-xs text-slate-500">
        {dailyUsedText}
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto space-y-4 pb-4">
        {messages.length === 0 && (
          <div className="text-center py-16">
            <p className="text-slate-500 text-sm mb-4">
              {t.ask.noQuestions}
            </p>
            <div className="space-y-2">
              {suggestions.map((suggestion) => (
                <button
                  key={suggestion}
                  onClick={() => setInput(suggestion)}
                  className="cursor-pointer block mx-auto text-sm text-brand-400 hover:text-brand-300 transition-colors focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none"
                >
                  &quot;{suggestion}&quot;
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div
            key={i}
            className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
          >
            <div
              className={`max-w-[85%] rounded-xl px-4 py-3 ${
                msg.role === 'user'
                  ? 'bg-brand-600 text-white'
                  : 'border border-slate-700/50 bg-slate-800/50 text-slate-300'
              }`}
            >
              {msg.isLoading && (
                <div className="flex items-center gap-2 text-sm text-slate-400">
                  <div className="h-2 w-2 rounded-full bg-brand-500 animate-pulse" />
                  {t.ask.thinking}
                </div>
              )}

              {msg.error && (
                <p className="text-sm text-red-400">{t.ask.error}: {msg.error}</p>
              )}

              {!msg.isLoading && !msg.error && msg.role === 'assistant' && (
                <>
                  <div className="text-sm leading-relaxed prose prose-invert prose-sm max-w-none prose-p:my-1 prose-strong:text-slate-100 prose-ul:my-1 prose-li:my-0">
                    <Markdown>{msg.content}</Markdown>
                  </div>

                  {msg.response && (
                    <>
                      <DataCitations response={msg.response} t={t} />
                      <div className="flex items-center gap-3 mt-3">
                        <TierBadge tier={msg.response.tier_used} t={t} />
                        {msg.response.llm_tokens_used > 0 && (
                          <span className="text-[10px] text-slate-500">
                            {msg.response.llm_tokens_used.toLocaleString()} {t.ask.tokens}
                          </span>
                        )}
                      </div>
                    </>
                  )}
                </>
              )}

              {msg.role === 'user' && (
                <p className="text-sm">{msg.content}</p>
              )}
            </div>
          </div>
        ))}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="sticky bottom-0 py-4 bg-slate-900/80 backdrop-blur-sm border-t border-slate-800">
        <form onSubmit={handleSubmit} className="flex gap-2">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder={placeholder}
            disabled={isSubmitting}
            className="flex-1 rounded-lg border border-slate-700/50 bg-slate-800/50 px-4 py-2.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-brand-500/50 focus:ring-1 focus:ring-brand-500/50 disabled:opacity-50"
          />
          <button
            type="submit"
            disabled={isSubmitting || !input.trim()}
            className="cursor-pointer rounded-lg bg-brand-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-brand-500 transition-colors disabled:opacity-50 disabled:cursor-not-allowed focus-visible:ring-2 focus-visible:ring-brand-500 focus-visible:outline-none"
          >
            {t.ask.send}
          </button>
        </form>

        <p className="text-[10px] text-slate-600 mt-2 text-center">
          {disclaimer}
        </p>
      </div>
    </div>
  );
}
