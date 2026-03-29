export type Language = "en" | "pt";

export const translations = {
  en: {
    nav: {
      home: "Home",
      askAi: "Ask AI",
      dashboard: "Dashboard",
      analytics: "Analytics",
      about: "About",
      quality: "Quality",
    },
    hero: {
      title: "Veredas",
      dataQualityLink: "View data quality",
    },
    landing: {
      ctaPrimary: "Ask a question",
      ctaSecondary: "Explore the data",
      featuresTitle: "Features",
      dataSourcesTitle: "Data Sources",
      howItWorksTitle: "How It Works",
      step1Title: "Explore the data",
      step1Desc:
        "Browse 48 indicators across monetary policy, inflation, labor, output, and bond markets — all from official Brazilian sources.",
      step2Title: "Ask questions",
      step2Desc:
        "Type a question in plain language. The AI pulls verified data and gives you a sourced answer in seconds.",
      step3Title: "Stay informed",
      step3Desc:
        "Daily AI-generated insights and weekly anomaly alerts. Never miss a COPOM decision or an inflation surprise.",
      openSourceTitle: "Fully Open Source",
      openSourceDesc:
        "Every line of code is public. Verify the data pipeline, audit the AI prompts, or deploy your own instance.",
      viewOnGithub: "View on GitHub",
      footerLinks: "Links",
    },
    insight: {
      title: "AI Insight Digest",
      noData:
        "No insights yet. Daily summaries will show up here once data is processed.",
      poweredBy: "Powered by",
      refs: "Refs",
    },
    ask: {
      title: "Ask AI",
      send: "Send",
      thinking: "Thinking...",
      noQuestions: "No questions yet. Try asking something like:",
      dailyUsed: "{count} of {limit} daily questions used",
      tier1: "Tier 1 - Direct Lookup",
      tier3: "Tier 3 - LLM",
      tokens: "tokens",
      dataPoints: "Data Points",
      sources: "Sources",
      error: "Error",
    },
    analytics: {
      title: "Analytics Dashboard",
      subtitle: "Historical time series for all tracked economic indicators.",
      exportCsv: "Export CSV",
      points: "pts",
      noData: "No data available",
      failedToLoad: "Failed to load data",
      lastUpdated: "Last updated",
      dataPoints: "data points",
    },
    quality: {
      title: "Pipeline Quality",
      subtitle: "Pipeline run status, data freshness, and sync health.",
      systemHealth: "System Health",
      qualityStatus: "Quality Status",
      syncStatus: "R2 Sync Status",
      dataFreshness: "Data Freshness",
      dataFreshnessHealth: "Data Freshness (from Health Check)",
      seriesFreshness: "Series Freshness",
      lastSync: "Last Sync",
      filesSynced: "Files Synced",
      syncLag: "Sync Lag",
      syncHealth: "Sync Health",
      runId: "Run ID",
      status: "Status",
      timestamp: "Timestamp",
      duration: "Duration",
      source: "Source",
      noData: "No data available",
      unableToLoadSync: "Unable to load sync status",
      unableToLoadQuality: "Unable to load quality status",
      pipelineQuality: "Pipeline Quality",
    },
    about: {
      whatIsTitle: "What is Veredas",
      whatIsDesc:
        "Veredas collects macroeconomic data from 8 official Brazilian sources — Banco Central, IBGE, Tesouro Nacional, FRED and more — validates it daily, and makes it queryable through AI. Ask a question in plain language, get a sourced answer.",
      dataSources: "Data Sources",
      openSource: "Open Source",
      openSourceDesc:
        "The entire codebase is public on GitHub. Verify how data is collected, audit the AI prompts, or deploy your own instance.",
      forDevelopers: "For Developers",
      techStack: "Tech Stack",
      backend: "Backend",
      frontend: "Frontend",
      aiMl: "AI / ML",
      infrastructure: "Infrastructure",
      author: "Author",
      authorBio:
        "Built by Alex Monteiro to fill the gap between free government data APIs and expensive financial terminals.",
    },
    anomaly: {
      title: "Anomaly Analysis",
      noData:
        "No anomaly analysis available yet. It will appear after the next pipeline run detects statistical outliers.",
      poweredBy: "Powered by",
      badge: "Macro Context",
    },
    common: {
      fresh: "Fresh",
      stale: "Stale",
      critical: "Critical",
      noDataAvailable: "No data available",
      loading: "Loading...",
    },
  },
  pt: {
    nav: {
      home: "Início",
      askAi: "Pergunte à IA",
      dashboard: "Painel",
      analytics: "Analítico",
      about: "Sobre",
      quality: "Qualidade",
    },
    hero: {
      title: "Veredas",
      dataQualityLink: "Ver qualidade dos dados",
    },
    landing: {
      ctaPrimary: "Faça uma pergunta",
      ctaSecondary: "Explorar os dados",
      featuresTitle: "Funcionalidades",
      dataSourcesTitle: "Fontes de Dados",
      howItWorksTitle: "Como Funciona",
      step1Title: "Explore os dados",
      step1Desc:
        "Navegue por 48 indicadores de política monetária, inflação, trabalho, atividade e títulos públicos — todos de fontes oficiais brasileiras.",
      step2Title: "Faça perguntas",
      step2Desc:
        "Escreva uma pergunta em linguagem natural. A IA busca dados verificados e entrega uma resposta com fontes em segundos.",
      step3Title: "Fique informado",
      step3Desc:
        "Insights diários gerados por IA e alertas semanais de anomalias. Nunca perca uma decisão do COPOM ou uma surpresa inflacionária.",
      openSourceTitle: "Totalmente Open Source",
      openSourceDesc:
        "Todo o código é público. Verifique o pipeline de dados, audite os prompts de IA ou implante sua própria instância.",
      viewOnGithub: "Ver no GitHub",
      footerLinks: "Links",
    },
    insight: {
      title: "Resumo de IA",
      noData:
        "Ainda sem resumos. Os resumos diários aparecerão aqui quando os dados forem processados.",
      poweredBy: "Gerado por",
      refs: "Refs",
    },
    ask: {
      title: "Pergunte à IA",
      send: "Enviar",
      thinking: "Pensando...",
      noQuestions: "Nenhuma pergunta ainda. Tente perguntar algo como:",
      dailyUsed: "{count} de {limit} perguntas diárias usadas",
      tier1: "Nível 1 - Consulta Direta",
      tier3: "Nível 3 - LLM",
      tokens: "tokens",
      dataPoints: "Pontos de Dados",
      sources: "Fontes",
      error: "Erro",
    },
    analytics: {
      title: "Painel Analítico",
      subtitle:
        "Séries temporais históricas para todos os indicadores econômicos monitorados.",
      exportCsv: "Exportar CSV",
      points: "pts",
      noData: "Dados indisponíveis",
      failedToLoad: "Falha ao carregar dados",
      lastUpdated: "Última atualização",
      dataPoints: "pontos de dados",
    },
    quality: {
      title: "Qualidade do Pipeline",
      subtitle:
        "Status de execução do pipeline, frescor dos dados e saúde da sincronização.",
      systemHealth: "Saúde do Sistema",
      qualityStatus: "Status de Qualidade",
      syncStatus: "Status de Sincronização R2",
      dataFreshness: "Frescor dos Dados",
      dataFreshnessHealth: "Frescor dos Dados (do Health Check)",
      seriesFreshness: "Frescor das Séries",
      lastSync: "Última Sincronização",
      filesSynced: "Arquivos Sincronizados",
      syncLag: "Atraso de Sincronização",
      syncHealth: "Saúde da Sincronização",
      runId: "ID de Execução",
      status: "Status",
      timestamp: "Data/Hora",
      duration: "Duração",
      source: "Origem",
      noData: "Dados indisponíveis",
      unableToLoadSync: "Não foi possível carregar o status de sincronização",
      unableToLoadQuality: "Não foi possível carregar o status de qualidade",
      pipelineQuality: "Qualidade do Pipeline",
    },
    about: {
      whatIsTitle: "O que é o Veredas",
      whatIsDesc:
        "O Veredas coleta dados macroeconômicos de 8 fontes oficiais brasileiras — Banco Central, IBGE, Tesouro Nacional, FRED e mais — valida diariamente e torna consultável por IA. Faça uma pergunta em linguagem natural, receba uma resposta com fontes.",
      dataSources: "Fontes de Dados",
      openSource: "Código Aberto",
      openSourceDesc:
        "Todo o código é público no GitHub. Verifique como os dados são coletados, audite os prompts de IA ou implante sua própria instância.",
      forDevelopers: "Para Desenvolvedores",
      techStack: "Stack Tecnológico",
      backend: "Backend",
      frontend: "Frontend",
      aiMl: "IA / ML",
      infrastructure: "Infraestrutura",
      author: "Autor",
      authorBio:
        "Desenvolvido por Alex Monteiro para preencher a lacuna entre APIs públicas gratuitas e terminais financeiros caros.",
    },
    anomaly: {
      title: "Análise de Anomalias",
      noData:
        "Análise de anomalias ainda indisponível. Aparecerá após a próxima execução do pipeline detectar outliers estatísticos.",
      poweredBy: "Gerado por",
      badge: "Contexto Macro",
    },
    common: {
      fresh: "Atualizado",
      stale: "Desatualizado",
      critical: "Crítico",
      noDataAvailable: "Dados indisponíveis",
      loading: "Carregando...",
    },
  },
} as const;

/** Widen literal string types to `string` and readonly arrays to `readonly string[]` */
type DeepStringify<T> = T extends readonly string[]
  ? readonly string[]
  : T extends string
    ? string
    : T extends object
      ? { readonly [K in keyof T]: DeepStringify<T[K]> }
      : T;

export type Translations = DeepStringify<(typeof translations)["en"]>;
